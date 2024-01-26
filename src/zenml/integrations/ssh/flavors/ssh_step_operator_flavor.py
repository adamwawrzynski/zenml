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
"""SSH step operator flavor."""

from typing import TYPE_CHECKING, Optional, Type, Dict, Any, List

from zenml.config.base_settings import BaseSettings
from zenml.integrations.ssh import (
    SSH_STEP_OPERATOR_FLAVOR,
)
from zenml.step_operators.base_step_operator import (
    BaseStepOperatorConfig,
    BaseStepOperatorFlavor,
)

if TYPE_CHECKING:
    from zenml.integrations.ssh.step_operators import SSHStepOperator


class SSHStepOperatorSettings(BaseSettings):
    """Settings for the SSH step operator.

    Attributes:

    """
    zenml_server_url: Optional[str]
    zenml_server_username: Optional[str]
    zenml_server_password: Optional[str]
    stack_name: Optional[str]
    username: Optional[str]
    port: Optional[int]
    ip_address: Optional[str]
    requirements: List[str] = []
    integrations: List[str] = []
    connect_kwargs: Dict[str, Any] = {}
    use_virtualenv: bool = False
    virtualenv_path: Optional[str] = None


class SSHStepOperatorConfig(  # type: ignore[misc] # https://github.com/pydantic/pydantic/issues/4173
    BaseStepOperatorConfig, SSHStepOperatorSettings
):
    """Config for the SSH step operator.
    """


    @property
    def is_remote(self) -> bool:
        """Checks if this stack component is running remotely.

        This designation is used to determine if the stack component can be
        used with a local ZenML database or if it requires a remote ZenML
        server.

        Returns:
            True if this config is for a remote component, False otherwise.
        """
        return True


class SSHStepOperatorFlavor(BaseStepOperatorFlavor):
    """Flavor for the Sagemaker step operator."""

    @property
    def name(self) -> str:
        """Name of the flavor.

        Returns:
            The name of the flavor.
        """
        return SSH_STEP_OPERATOR_FLAVOR

    @property
    def docs_url(self) -> Optional[str]:
        """A url to point at docs explaining this flavor.

        Returns:
            A flavor docs url.
        """
        return self.generate_default_docs_url()

    @property
    def sdk_docs_url(self) -> Optional[str]:
        """A url to point at SDK docs explaining this flavor.

        Returns:
            A flavor SDK docs url.
        """
        return self.generate_default_sdk_docs_url()

    @property
    def config_class(self) -> Type[SSHStepOperatorConfig]:
        """Returns SagemakerStepOperatorConfig config class.

        Returns:
            The config class.
        """
        return SSHStepOperatorConfig

    @property
    def implementation_class(self) -> Type["SSHStepOperator"]:
        """Implementation class.

        Returns:
            The implementation class.
        """
        from zenml.integrations.ssh.step_operators import SSHStepOperator

        return SSHStepOperator
