# cp_client/remote.py
import asyncio
import logging
import math
from typing import Optional

from ocpp.v201.enums import (
    ConnectorStatusEnumType,
    TriggerReasonEnumType,
    MeasurandEnumType,
    ReasonEnumType,
)

from store.state import state
from store.meters import meters
from store.conf_keys import configuration_keys
from config.settings import settings
from cp_client.context import set_connector_id, set_transaction_id

logger = logging.getLogger("remote")


class RemoteCommandHandler:
    """
    Gerencia o processamento assíncrono de comandos remotos (RemoteStart,
    RemoteStop, UnlockConnector) e as tasks de recarga associadas.
    """

    def __init__(self, charge_point):
        self.cp = charge_point
        self._recharge_tasks = {}  # transaction_id -> asyncio.Task

    async def handle_remote_start(self, id_tag: str, remote_start_id: int, connector_id: int, charging_profile=None):
        """Executa a sequência de autorização e início de transação."""
        """DE ACORDO COM O CENÁRIO ---   F02   ---""" 
        set_connector_id(connector_id)
        try:

            started = await self.cp.start_transaction(connector_id, id_tag, remote_start_id=remote_start_id)
            if not started:
                logger.error(f"Falha ao iniciar transação para tag {id_tag}")
                state.update_connector_transaction_id(connector_id, None)
            else:
                logger.info(f"Transação iniciada com sucesso: transaction_id={remote_start_id}")
                await self.cp.send_status_notification(
                                evse_id=1,
                                connector_id=connector_id,
                                status=ConnectorStatusEnumType.occupied,
                            )
                state.update_connector_status(evse_id=1, connector_id=connector_id, status=ConnectorStatusEnumType.occupied)
                self._start_recharge_task(connector_id, remote_start_id)

        finally:
            set_connector_id(None)

    async def handle_remote_stop(self, transaction_id, connector_id):
        set_connector_id(connector_id)
        try:
            await self._stop_recharge_task(transaction_id)
            await self.cp.stop_transaction(connector_id, transaction_id, reason= ReasonEnumType.remote, trigger_reason = TriggerReasonEnumType.remote_stop)

            await self.cp.send_status_notification(
                                evse_id=1,
                                connector_id=connector_id,
                                status=ConnectorStatusEnumType.available,
                            )
            state.update_connector_status(evse_id=1, connector_id=connector_id, status=ConnectorStatusEnumType.available)
            logger.info(f"RemoteStopTransaction processado com sucesso para transaction_id={transaction_id}")
        except Exception as e:
            logger.error(f"Erro ao processar RemoteStop: {e}")
        finally:
            set_connector_id(None)

    async def handle_unlock_connector(self, connector_id: int):
        """Processa UnlockConnector em background."""
        # Verifica se o conector ainda está ocupado (pode ter mudado)
        connector_status = state.get_connector_status(connector_id)
        if connector_status == (ConnectorStatusEnumType.charging or ConnectorStatusEnumType.suspended_ev or ConnectorStatusEnumType.suspended_evse):
            logger.warning(f"Conector {connector_id} está em uso, não pode ser desbloqueado")
            return
        # Em um simulador, apenas registramos
        logger.info(f"Conector {connector_id} desbloqueado")
        # Opcional: notificar servidor? A especificação não exige.

    # ----------------------------------------------------------------------
    # Gerenciamento das tasks de recarga
    # ----------------------------------------------------------------------
    def _start_recharge_task(self, connector_id: int, transaction_id: int):
        """Inicia e armazena a task de recarga."""
        if transaction_id in self._recharge_tasks:
            logger.warning(f"Task de recarga já existe para transaction_id={transaction_id}")
            return
        task = asyncio.create_task(self._recharge_loop(connector_id, transaction_id))
        self._recharge_tasks[transaction_id] = task
        logger.info(f"Task de recarga iniciada para transaction_id={transaction_id}")

    async def _stop_recharge_task(self, transaction_id: int):
        """Cancela a task de recarga e aguarda sua finalização."""
        task = self._recharge_tasks.pop(transaction_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logger.info(f"Task de recarga cancelada para transaction_id={transaction_id}")

    async def _recharge_loop(self, connector_id: int, transaction_id: int):
        """
        Loop contínuo de atualização do medidor e envio de MeterValues.
        A task é cancelada quando RemoteStop é processado.
        """
        logger.info(f"Iniciando loop de recarga para transaction_id={transaction_id}")
        try:
            sampling_interval = configuration_keys.get("MeterValueSampleInterval")

            inst_volt=float(meters.get_value(connector_id, MeasurandEnumType.voltage))
            inst_curr=float(meters.get_value(connector_id, MeasurandEnumType.current_import))
            
            inst_pot=inst_volt*inst_curr

            # Energia por intervalo (Wh)
            energy_per_interval = inst_pot * (sampling_interval / 3600.0)

            while True:
                # Marca o início do intervalo
                start_time = asyncio.get_event_loop().time()

                try:
                    # Aguarda o intervalo, podendo ser cancelado
                    await asyncio.sleep(sampling_interval / settings.time_scale)
                except asyncio.CancelledError:
                    # Calcula o tempo efetivamente decorrido desde o início do intervalo
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed > 0:
                        # Energia proporcional ao tempo decorrido
                        partial_energy = inst_pot * (elapsed / 3600.0)  # Wh
                        if partial_energy > 0:
                            meters.update_active_import_register(connector_id, round(partial_energy, 2))
                            await self.cp.send_transaction_meter_values(connector_id, transaction_id)
                    # Relança para propagar o cancelamento
                    raise

                # Se chegou aqui, o sleep completo sem cancelamento
                meters.update_active_import_register(connector_id, round(energy_per_interval, 2))
                await self.cp.send_transaction_meter_values(connector_id)

        except asyncio.CancelledError:
            logger.info(f"Loop de recarga cancelado para transaction_id={transaction_id}")
            raise
        except Exception as e:
            logger.error(f"Erro no loop de recarga: {e}", exc_info=True)

    # ----------------------------------------------------------------------
    # Parada limpa de todas as tasks
    # ----------------------------------------------------------------------
    async def cancel_all_recharge_tasks(self):
        """Cancela todas as tasks de recarga ativas."""
        for transaction_id, task in list(self._recharge_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._recharge_tasks.clear()
        logger.info("Todas as tasks de recarga foram canceladas")