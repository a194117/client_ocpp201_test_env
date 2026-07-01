# scenarios/failed_charging.py
import asyncio
import logging

from cp_client.client import ChargePoint
from cp_client.context import set_connector_id
from config.settings import settings
from store.state import state
from .base import Scenario, Parameter

from ocpp.v201.enums import ConnectorStatusEnumType

logger = logging.getLogger("scenarios")

class FailedChargingScenario(Scenario):
    
    _failed_charging_parameters = [
        Parameter("id_tag", default="A1B2C3D4", p_type="str", description="Tag do usuário"),
        Parameter("connector_id", default="1", p_type="int", description="ID do conector"),
    ]
    
    def __init__(self):
        super().__init__("failed_charging", self._failed_charging_parameters, True)

    async def execute(self, cp: ChargePoint, **kwargs) -> bool:
        # Obtém os argumentos id_tag & connector_id  (com fallback)
        recharge_value = kwargs.get('recharge_value', settings.recharge_value)
        id_tag = kwargs.get('id_tag', settings.id_tag)
        connector_id = kwargs.get('connector_id', 1)
        

        # 1. Authorize
        auth_ok = await cp.authorize(id_tag)
        if not auth_ok:
            logger.error(f"Authorization failed for tag {id_tag}")
            set_connector_id(None)
            return False
        

        # 2. StartTransaction
        started = await cp.start_transaction(connector_id, id_tag)
        if not started:
            logger.error(f"It is not possible to start the transaction for tag {id_tag}")
            set_connector_id(None)
            return False
        else:
            state.update_connector_status(1, connector_id, ConnectorStatusEnumType.occupied)
            
        # 3. MeterValues durante a transação

        stop_reason = await self.perform_recharge(cp.send_transaction_meter_values, recharge_value, connector_id, id_token=id_tag)

        # 4. StopTransaction
        await cp.stop_transaction(connector_id, id_tag, reason=stop_reason)
        state.update_connector_status(1, connector_id, ConnectorStatusEnumType.available)

        set_connector_id(None)
        return True