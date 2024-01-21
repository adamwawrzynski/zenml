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

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Type, cast

from fabric import Connection

from zenml.client import Client
from zenml.config.build_configuration import BuildConfiguration
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
from zenml.utils.string_utils import random_str

if TYPE_CHECKING:
    from zenml.config.base_settings import BaseSettings
    from zenml.config.step_run_info import StepRunInfo
    from zenml.models import PipelineDeploymentBase

logger = get_logger(__name__)

# SAGEMAKER_DOCKER_IMAGE_KEY = "sagemaker_step_operator"
_ENTRYPOINT_ENV_VARIABLE = "__ZENML_ENTRYPOINT"


class SSHStepOperator(BaseStepOperator):
    """Step operator to run a step on Sagemaker.

    This class defines code that builds an image with the ZenML entrypoint
    to run using Sagemaker's Estimator.
    """

    @property
    def config(self) -> SSHStepOperatorConfig:
        """Returns the `SagemakerStepOperatorConfig` config.

        Returns:
            The configuration.
        """
        return cast(SSHStepOperatorConfig, self._config)

    @property
    def settings_class(self) -> Optional[Type["BaseSettings"]]:
        """Settings class for the SageMaker step operator.

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

    # @property
    # def validator(self) -> Optional[StackValidator]:
    #     """Validates the stack.

    #     Returns:
    #         A validator that checks that the stack contains a remote container
    #         registry and a remote artifact store.
    #     """

    #     def _validate_remote_components(stack: "Stack") -> Tuple[bool, str]:
    #         if stack.artifact_store.config.is_local:
    #             return False, (
    #                 "The SageMaker step operator runs code remotely and "
    #                 "needs to write files into the artifact store, but the "
    #                 f"artifact store `{stack.artifact_store.name}` of the "
    #                 "active stack is local. Please ensure that your stack "
    #                 "contains a remote artifact store when using the SageMaker "
    #                 "step operator."
    #             )

    #         container_registry = stack.container_registry
    #         assert container_registry is not None

    #         if container_registry.config.is_local:
    #             return False, (
    #                 "The SageMaker step operator runs code remotely and "
    #                 "needs to push/pull Docker images, but the "
    #                 f"container registry `{container_registry.name}` of the "
    #                 "active stack is local. Please ensure that your stack "
    #                 "contains a remote container registry when using the "
    #                 "SageMaker step operator."
    #             )

    #         return True, ""

    #     return StackValidator(
    #         required_components={
    #             StackComponentType.CONTAINER_REGISTRY,
    #             StackComponentType.IMAGE_BUILDER,
    #         },
    #         custom_validation_function=_validate_remote_components,
    #     )

    def launch(
        self,
        info: "StepRunInfo",
        entrypoint_command: List[str],
        environment: Dict[str, str],
    ) -> None:
        """Launches a step on SageMaker.

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
        connection = Connection(
            settings.ip_address,
            user=settings.user,
            port=settings.port,
            connect_kwargs={
                "password": settings.password,
            }
        )
        # TODO: copy directory with code to host machine
        # TODO: what about dependencies/requirements to run job?
        connection.run("uname -a"), hide=True)
