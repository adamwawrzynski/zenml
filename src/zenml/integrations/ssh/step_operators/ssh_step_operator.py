#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Implementation of the Sagemaker Step Operator."""

import os
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, cast

from uuid import uuid4
import subprocess

from fabric import Connection

from zenml.enums import StackComponentType
from zenml.integrations.ssh.flavors.ssh_step_operator_flavor import (
    SSHStepOperatorConfig,
    SSHStepOperatorSettings,
)
from zenml.integrations.ssh.step_operators.ssh_step_operator_entrypoint_config import (
    SSH_ESTIMATOR_STEP_ENV_VAR_SIZE_LIMIT,
    SSHEntrypointConfiguration,
)
from zenml.logger import get_logger
from zenml.stack import Stack, StackValidator
from zenml.step_operators import BaseStepOperator
from zenml.step_operators.step_operator_entrypoint_configuration import (
    StepOperatorEntrypointConfiguration,
)
from zenml.utils.env_utils import split_environment_variables

if TYPE_CHECKING:
    from zenml.config.base_settings import BaseSettings
    from zenml.config.step_run_info import StepRunInfo

logger = get_logger(__name__)

_ENTRYPOINT_ENV_VARIABLE = "__ZENML_ENTRYPOINT"


class SSHStepOperator(BaseStepOperator):
    """Step operator to run a step over SSH.

    This class defines code that creates environment with the ZenML entrypoint
    to run using remote server over SSH.
    """

    @property
    def config(self) -> SSHStepOperatorConfig:
        """Returns the `SSHStepOperatorConfig` config.

        Returns:
            The configuration.
        """
        return cast(SSHStepOperatorConfig, self._config)

    @property
    def settings_class(self) -> Optional[Type["BaseSettings"]]:
        """Settings class for the SSH step operator.

        Returns:
            The settings class.
        """
        return SSHStepOperatorSettings

    @property
    def entrypoint_config_class(
        self,
    ) -> Type[StepOperatorEntrypointConfiguration]:
        """Returns the entrypoint configuration class for this step operator.

        Returns:
            The entrypoint configuration class for this step operator.
        """
        return SSHEntrypointConfiguration

    @property
    def validator(self) -> Optional[StackValidator]:
        """Validates the stack.

        Returns:
            A validator that checks that the stack containsa remote artifact
            store.
        """

        def _validate_remote_components(stack: "Stack") -> Tuple[bool, str]:
            if stack.artifact_store.config.is_local:
                return False, (
                    "The SSH step operator runs code remotely and "
                    "needs to write files into the artifact store, but the "
                    f"artifact store `{stack.artifact_store.name}` of the "
                    "active stack is local. Please ensure that your stack "
                    "contains a remote artifact store when using the SSH "
                    "step operator."
                )

            return True, ""

        return StackValidator(
            required_components={
                StackComponentType.ARTIFACT_STORE,
            },
            custom_validation_function=_validate_remote_components,
        )

    def launch(
        self,
        info: "StepRunInfo",
        entrypoint_command: List[str],
        environment: Dict[str, str],
    ) -> None:
        """Launches a step on remote machine over SSH.

        Args:
            info: Information about the step run.
            entrypoint_command: Command that executes the step.
            environment: Environment variables to set in the step operator
                environment.

        Raises:
            RuntimeError: If the connector returns an object that is not a
                `boto3.Session`.
        """
        split_environment_variables(
            env=environment,
            size_limit=SSH_ESTIMATOR_STEP_ENV_VAR_SIZE_LIMIT,
        )

        environment[_ENTRYPOINT_ENV_VARIABLE] = " ".join(entrypoint_command)
        settings = cast(SSHStepOperatorSettings, self.get_settings(info))
        run_uuid = str(uuid4())

        code_directory = settings.code_directory
        connection = Connection(
            settings.ip_address,
            user=settings.username,
            port=settings.port,
            connect_kwargs=settings.connect_kwargs
        )


        run_dir = os.path.join("/home/", run_uuid)
        connection.run(f"mkdir {run_dir}", hide=True)

        # create archive of project code
        subprocess.call(f"tar -czvf {run_uuid}.tar.gz {code_directory}", shell=True)

        # copy project code archive via SSH to remote machine
        connection.put(f"{run_uuid}.tar.gz", run_dir)

        # remove local archive
        # subprocess.call(f"rm {run_uuid}.tar.gz")

        with connection.cd(run_dir):
            requirements_file_content = "\n".join(settings.requirements)
            connection.run(f"echo \"{requirements_file_content}\" > requirements.txt", hide=True)

            # if provided activate virtual environment
            if settings.use_virtualenv and settings.virtualenv_path:
                connection.run(f"source {settings.virtualenv_path}", hide=False)

            # TODO: for testing purpouses after first exectution of this steps on
            # remote machine you have to manually copy directory with added SSH
            # step operator to zenml/integrations/ directory and copy updated
            # zenml/integrations/constants.py file
            # after first execution I recommend to comment out those lines to prevent
            # overwriting modified zenml package
            # # install dependencies
            connection.run("python -m pip install --upgrade pip", hide=False)
            connection.run("python -m pip install -r requirements.txt", hide=False)

            # # install integrations
            integration_string = " ".join(settings.integrations)
            connection.run(f"zenml integration install {integration_string} -y", hide=False)

            # connect to ZenML Server
            connection.run(f"zenml connect --url {settings.zenml_server_url} --username {settings.zenml_server_username} --password {settings.zenml_server_password}", hide=False)

            # activate stack
            connection.run(f"zenml stack set {settings.stack_name}", hide=False)

            # unpack archive with project code
            connection.run(f"tar -xzvf {run_uuid}.tar.gz")

            # remove archive
            connection.run(f"rm {run_uuid}.tar.gz")

            # TODO: somehow we need package similarly to the command executed locally
            # but also need the code from visability of entrypoint script
            # for now I copy content of project directory to the local directory
            connection.run(f"cp -r {code_directory}/* .")

            # run step execution command
            connection.run(environment[_ENTRYPOINT_ENV_VARIABLE], hide=False)

        # clean environment after execution
        connection.run(f"rm -rf {run_dir}")

        if settings.use_virtualenv:
            # deactivate virtual environment
            connection.run("deactivate", hide=False)
