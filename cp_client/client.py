# cp_client/client.py
import asyncio
import logging
from datetime import timezone
from typing import Optional, Callable, Awaitable, Any, Dict, List

import websockets
from ocpp.routing import on
from ocpp.v201 import ChargePoint as BaseChargePoint
from ocpp.v201  import call
from ocpp.v201 import call_result
from ocpp.v201.enums import (
    BootReasonEnumType, 
    RegistrationStatusEnumType, 
    ConnectorStatusEnumType,
    IdTokenEnumType,
    AuthorizationStatusEnumType,
    TransactionEventEnumType,
    TriggerReasonEnumType,
    ReadingContextEnumType,
    MeasurandEnumType,
    ReasonEnumType,
    Action,
    RequestStartStopStatusEnumType,
    UnlockStatusEnumType,
    LocationEnumType,
    PhaseEnumType,
    GetChargingProfileStatusEnumType,
)

from store.state import state
from store.conf_keys import configuration_keys
from store.meters import meters
from config.settings import settings
from store.conf_keys import configuration_keys
from .base import setup_logger
from .remote import RemoteCommandHandler
from .context import set_transaction_id, get_connector_id, set_connector_id

logger = setup_logger("cp_client")

class ChargePoint(BaseChargePoint):
    def __init__(self, station_id: str, connection, response_timeout=30):
        super().__init__(station_id, connection, response_timeout)
        self.station_id = station_id
        self._stop_requested = False
        self.connector_id = 1
        self.evse_id = 1

        self._command_queue = asyncio.Queue()
        self._command_processor_task = None

        self.remote_handler = RemoteCommandHandler(self)

    async def start(self):
        self._command_processor_task = asyncio.create_task(self._process_command_queue())
        """Sobrescreve start para iniciar o loop de mensagens."""
        await super().start()

    async def send_boot_notification(self) -> bool:
        """Envia BootNotification e aguarda aceitação."""
        if state.registration:
            logger.debug("BootNotification ja foi aceito anteriormente")
            return True
            

        modem_kwargs = { }
        if settings.iccid:
            charging_station_kwargs["iccid"] = settings.iccid
        if settings.imsi:
            charging_station_kwargs["imsi"] = settings.imsi

        charging_station_kwargs = {
            "model": settings.charge_point_model,
            "vendor_name": settings.charge_point_vendor,
        }    
        if settings.charge_point_serial_number:
            charging_station_kwargs["serial_number"] = settings.charge_point_serial_number
        if settings.firmware_version:
            charging_station_kwargs["firmware_version"] = settings.firmware_version
        charging_station_kwargs["modem"] = modem_kwargs


        boot_kwargs = {
            "reason": BootReasonEnumType.power_up,
            "charging_station": charging_station_kwargs
        }    

        request = call.BootNotification(**boot_kwargs)
        
        try:
            response = await self.call(request)
            logger.info(f"SendBootNotification.conf recebido com 'status': '{response.status}'")

            try:
                state.update(registration=response.status)
                if response.interval:
                    configuration_keys.set("HeartbeatInterval", response.interval)
                
                if state.registration == RegistrationStatusEnumType.rejected:
                    return False
                else:          
                    state.update_time_from_server(response.current_time)
                    return True
            
            except ValueError:
                raise Exception(f"'{response.status}' não é um estado de registro válido.")
                               
        except Exception as e:
            logger.error(f"Erro no BootNotification.req: {e}")
            return False

    async def send_status_notification(self, evse_id: int,  connector_id: int, status: ConnectorStatusEnumType):
        """
        Envia uma mensagem StatusNotification para um conector específico.
        """

        actual_time = state.get_current_time()

        status_kwargs = {
            "timestamp": actual_time,
            "connector_status": status, 
            "connector_id": connector_id, 
            "evse_id": evse_id
        }

        request = call.StatusNotification(**status_kwargs)

        try:
            response = await self.call(request)
            logger.info(f"StatusNotification.req enviado com 'status': '{status.value}'")
        except Exception as e: 
            logger.error(f"Falha ao enviar StatusNotification.req: {e}")
            raise 


    async def send_heartbeat(self):
        """Envia Heartbeat periodicamente, usando o intervalo definido pelo servidor."""
        try:
            while not self._stop_requested:
                """Utiliza Time Scale para adequar ao tempo de simulação."""
                await asyncio.sleep(configuration_keys.get("HeartbeatInterval")/settings.time_scale)
                try:
                    request = call.Heartbeat()
                    response = await self.call(request)
                    state.update_time_from_server(response.current_time)
                    logger.debug("Heartbeat.req enviado")
                except Exception as e:
                    logger.warning(f"Falha no Heartbeat.req: {e}")
                    break   # Sai do loop se houver erro (a tarefa será cancelada externamente)
        except asyncio.CancelledError:
            logger.info("Tarefa de heartbeat cancelada")
            self._stop_requested = True
            raise   # Re-lança para que a tarefa seja marcada como cancelada

    async def authorize(self, id_token: str) -> bool:
        token_type = IdTokenEnumType.iso14443

        logger.info(f"Enviando AuthorizeRequest para token {id_token}")
        request = call.Authorize(
            id_token={"idToken": id_token, "type": token_type},
        )
        try:
            authorize_response = await self.call(request)
            response_status = authorize_response.id_token_info.get('status')
            if response_status == AuthorizationStatusEnumType.accepted:
                logger.info(f"AuthorizeResponse 'status': {response_status}, 'idToken': {id_token}.")
                return True
            else:
                logger.warning(f"AuthorizeResponse 'status': {response_status}, 'idToken': {id_token}.")
                return False
        except Exception as e:
            logger.error(f"Erro durante AuthorizeResponse 'idToken': {id_token}. 'response': {authorize_response} Erro: {e}")
            return False
        
    async def send_transaction_event(
        self,
        event_type: TransactionEventEnumType,
        trigger_reason: TriggerReasonEnumType,
        timestamp: str,
        seq_no: int,
        transaction_info: Dict[str, Any],
        evse: Dict[str, int],
        meter_value: Optional[List[Dict[str, Any]]] = None,
        id_token: Optional[str] = None,
        **kwargs
    ) -> call_result.TransactionEvent:
        """
        Envia uma mensagem TransactionEvent para o CSMS com os parâmetros fornecidos.
        Os parâmetros 'timestamp' e 'event_type' são obrigatórios pela especificação.
        
        :param event_type: started, updated ou ended
        :param trigger_reason: motivo que disparou o evento
        :param seq_no: número sequencial da transação
        :param transaction_info: dicionário com transactionId e opcionalmente stoppingReason
        :param evse: dicionário com id e connectorId
        :param meter_value: lista de leituras do medidor (opcional, mas recomendado)
        :param id_token: token de identificação (obrigatório para started/ended)
        :param kwargs: parâmetros extras (ex.: offline, reservationId, etc.)
        :return: resposta do CSMS
        """
        request_params = {
            "event_type": event_type,
            "timestamp": timestamp,
            "trigger_reason": trigger_reason,
            "seq_no": seq_no,
            "transaction_info": transaction_info,
            "evse": evse,
            "meter_value": meter_value if meter_value is not None else [],
        }
        if id_token is not None:
            request_params["id_token"] = id_token

        request_params.update(kwargs)

        try:
            request = call.TransactionEvent(**request_params)
            response = await self.call(request)
            logger.debug(f"TransactionEvent {event_type} enviado. SeqNo: {seq_no}")
            return response
        except Exception as e:
            logger.error(f"Erro no TransactionEvent.req: {e}")
            return False
        
    async def start_transaction(self, connector_id: int, id_token_str: str, remote_start_id: Optional[int] = None, evse_id: Optional[int] = 1):
        sampled_values = meters.get_meter_value(
            connector_id=connector_id,
            measurands=None,
        )

        meter_start=int(float(sampled_values[0].get("value")))

        time_stamp=state.get_current_time()

        if remote_start_id is None:
            transaction_id = state.create_connector_transactionId(evse_id, connector_id)
            trigger_reason = TriggerReasonEnumType.authorized
        else:
            transaction_id = state.set_connector_transactionId(evse_id, connector_id, remote_start_id)
            trigger_reason = TriggerReasonEnumType.remote_start

        id_token_data = {"idToken": id_token_str, "type": IdTokenEnumType.iso14443}
        evse_data = {"id": evse_id, "connectorId": connector_id}
        transaction_info = {"transactionId": transaction_id}
        meter_value_start=[{
            "timestamp": time_stamp,
            "sampledValue": [{
                "value": meter_start,
                "context": ReadingContextEnumType.transaction_begin,
                "measurand": MeasurandEnumType.energy_active_import_register,
                "unitOfMeasure": {"unit": "kWh"},
            }],
        }]

        start_transaction_response  = await self.send_transaction_event(
            event_type=TransactionEventEnumType.started,
            timestamp=time_stamp,
            trigger_reason=trigger_reason,
            seq_no=0,
            transaction_info=transaction_info,
            id_token=id_token_data,
            evse=evse_data,
            meter_value=meter_value_start
        )
    
        if not start_transaction_response:
            state.clear_connector_transactionId(evse_id, connector_id)
            return False
        else :
            response_status = start_transaction_response.id_token_info.get('status')
            if response_status == AuthorizationStatusEnumType.accepted:
                logger.info(f"StartTransaction.conf 'status': {response_status}, 'meter_start' : {meter_start}")
                return True
            else:
                logger.warning(f"StartTransaction.conf 'status': {response_status}, 'meter_start' : {meter_start}")
                state.clear_connector_transactionId(evse_id, connector_id)
                return False
        
    async def stop_transaction(self, connector_id: int, id_token_str: Optional[str] = None, evse_id: Optional[int] = 1, reason: Optional[str] = None, trigger_reason: Optional[str] = TriggerReasonEnumType.stop_authorized):
        state.update_transaction_seq_no(evse_id, connector_id)
        
        sampled_values = meters.get_meter_value(
            connector_id=connector_id,
            measurands=None,
        )

        meter_stop=int(float(sampled_values[0].get("value")))

        conectorState = state.get_connector_state(evse_id, connector_id)
        transaction_id = conectorState["transaction_id"]
        seqNo = conectorState["transaction_seq_no"]

        time_stamp=state.get_current_time()

        if id_token_str is not None:
            id_token_data = {"idToken": id_token_str, "type": IdTokenEnumType.iso14443}
        evse_data = {"id": evse_id, "connectorId": connector_id}
        transaction_info_dict = {"transactionId": transaction_id, "stoppedReason": reason}
        meter_value_stop=[{
            "timestamp": time_stamp,
            "sampledValue": [{
                "value": meter_stop,
                "context": ReadingContextEnumType.transaction_end,
                "measurand": MeasurandEnumType.energy_active_import_register,
                "unitOfMeasure": {"unit": "kWh"},
            }],
        }]

        
        transaction_event_kwargs={
            "event_type": TransactionEventEnumType.ended,
            "timestamp": time_stamp,
            "trigger_reason": trigger_reason ,
            "seq_no": seqNo,
            "transaction_info": transaction_info_dict,
            "evse": evse_data,
            "meter_value":meter_value_stop
        }
        if id_token_str is not None:
            transaction_event_kwargs["id_token"] = id_token_data

        stop_transaction_response = await self.send_transaction_event(**transaction_event_kwargs)

        """
        stop_transaction_response = await self.send_transaction_event(
            event_type=TransactionEventEnumType.ended,
            timestamp=time_stamp,
            trigger_reason=trigger_reason ,
            seq_no=seqNo,
            transaction_info=transaction_info_dict,
            # id_token=id_token_data,
            evse=evse_data,
            meter_value=meter_value_stop
        )
        """

        state.clear_connector_transactionId(evse_id, connector_id)
        if not stop_transaction_response:
            return False
        else :
            logger.info(f"StopTransaction.conf 'meter_stop' : {meter_stop}")
            return True

    async def send_transaction_meter_values(self, connector_id: int, id_token_str: Optional[str] = None, evse_id: Optional[int] = 1):
        state.update_transaction_seq_no(evse_id, connector_id)

        sampled_values = meters.get_meter_value(
            connector_id=connector_id,
            measurands=configuration_keys.get("MeterValuesSampledData"),
            context=ReadingContextEnumType.sample_periodic
        )

        meter_updated=int(float(sampled_values[0].get("value")))

        conectorState = state.get_connector_state(evse_id, connector_id)
        transaction_id = conectorState["transaction_id"]
        seqNo = conectorState["transaction_seq_no"]

        time_stamp=state.get_current_time()

        if id_token_str is not None:
            id_token_data = {"idToken": id_token_str, "type": IdTokenEnumType.iso14443}
            
        evse_data = {"id": evse_id, "connectorId": connector_id}
        transaction_info_dict = {"transactionId": transaction_id}
        meter_value_updated=[{
            "timestamp": time_stamp,
            "sampledValue": [{
                "value": meter_updated,
                "context": ReadingContextEnumType.sample_periodic,
                "measurand": MeasurandEnumType.energy_active_import_register,
                "unitOfMeasure": {"unit": "kWh"},
            }],
        }]

        transaction_event_kwargs={
            "event_type": TransactionEventEnumType.updated,
            "timestamp": time_stamp,
            "trigger_reason": TriggerReasonEnumType.meter_value_periodic ,
            "seq_no": seqNo,
            "transaction_info": transaction_info_dict,
            "evse": evse_data,
            "meter_value":meter_value_updated
        }
        if id_token_str is not None:
            transaction_event_kwargs["id_token"] = id_token_data

        update_transaction_response = await self.send_transaction_event(**transaction_event_kwargs)

        """
        update_transaction_response = await self.send_transaction_event(
            event_type=TransactionEventEnumType.updated,
            timestamp=time_stamp,
            trigger_reason=TriggerReasonEnumType.meter_value_periodic ,
            seq_no=seqNo,
            transaction_info=transaction_info_dict,
            id_token=id_token_data,
            evse=evse_data,
            meter_value=meter_value_updated
        )
        """

        if not update_transaction_response:
            state.clear_connector_transactionId(evse_id, connector_id)
            return False
        else :
            logger.info(f"MeterValue Updated enviado medicao: {meter_updated}, 'context':{ReadingContextEnumType.sample_periodic}")

    @on(Action.get_charging_profiles)
    def on_get_charging_profiles(self, **kwargs):
        """
        Responde ao CSMS informando que o CP recebeu a solicitação
        e está processando. Como é um CP virtual, geralmente aceitamos
        ou retornamos 'Unknown' se não houver perfis configurados.
        """
        # Você pode logar os dados recebidos para depuração:
        print(f"GetChargingProfiles recebido: {kwargs}")

        return call_result.GetChargingProfiles(
            status=GetChargingProfileStatusEnumType.no_profiles
        )

    @on(Action.request_start_transaction)
    async def on_remote_start_transaction(self, id_token: str, remote_start_id: int, evse_id: int = 1, connector_id: int = 1, charging_profile=None):
        set_connector_id(connector_id)

        id_tag = id_token.pop("id_token")

        logger.info(f"RemoteStartTransaction recebido: id_tag={id_tag }, connector_id={connector_id}")

        actualStatus = state.get_connector_state(evse_id=evse_id, connector_id=connector_id).get("status")

        if actualStatus is not ConnectorStatusEnumType.available:
            logger.warning(f"Já existe transação ativa no conector {connector_id}")
            set_connector_id(None)
            return call_result.RequestStartTransaction(RequestStartStopStatusEnumType.rejected)
        
        await self._command_queue.put(('remote_start', id_tag, remote_start_id, connector_id, charging_profile))
        set_connector_id(None)
        return call_result.RequestStartTransaction(RequestStartStopStatusEnumType.accepted)
    
    @on(Action.request_stop_transaction)
    async def on_remote_stop_transaction(self, transaction_id: int):

        active_connector = state.is_transaction_active(transaction_id)

        if not active_connector:
            return call_result.RequestStopTransaction(RequestStartStopStatusEnumType.rejected)
        
        # Enfileira
        await self._command_queue.put(('remote_stop', transaction_id, active_connector))
        return call_result.RequestStopTransaction(RequestStartStopStatusEnumType.accepted)
    
    @on(Action.unlock_connector)
    async def on_unlock_connector(self, connector_id: int):
        # Verifica se o conector está ocupado (transação ativa?) – se sim, pode rejeitar
        #if self._is_connector_in_use(connector_id):
        #    return call_result.UnlockConnector(status=UnlockStatus.unlock_failed)  # ou NotSupported
        # Enfileira
        await self._command_queue.put(('unlock', connector_id))
        return call_result.UnlockConnector(status=UnlockStatusEnumType.unlocked)    

    async def _process_command_queue(self):
        # Formato do item: ('remote_start', id_tag, connector_id, charging_profile)
        # ou ('remote_stop', transaction_id, connector_id, id_tag, reason)
        # ou ('unlock', connector_id)
        while not self._stop_requested:
            try:
                item = await self._command_queue.get()
                if item is None:  # Sinal de parada
                    break

                command_type = item[0]
                if command_type == 'remote_start':
                    _, id_tag, remote_start_id, connector_id, charging_profile = item
                    await self.remote_handler.handle_remote_start(id_tag, remote_start_id, connector_id, charging_profile)
                elif command_type == 'remote_stop':
                    _, transaction_id, connector_id = item
                    await self.remote_handler.handle_remote_stop(transaction_id, connector_id)
                elif command_type == 'unlock':
                    _, connector_id = item
                    await self.remote_handler.handle_unlock_connector(connector_id)
                else:
                    logger.warning(f"Tipo de comando desconhecido: {command_type}")

            except asyncio.CancelledError:
                logger.info("Processador de comandos cancelado")
                break
            except Exception as e:
                logger.error(f"Erro no processador de comandos: {e}", exc_info=True)

async def run_charge_point_with_reconnect(
    on_connect: Optional[Callable[[ChargePoint], Awaitable[None]]] = None,
    on_disconnect: Optional[Callable[[], Awaitable[None]]] = None
):
    """
    Gerencia a conexão persistente do Charge Point com reconexão automática.
    Notifica via callbacks quando um novo ChargePoint é conectado ou quando a conexão é perdida.
    """
    retries = 0
    while retries < configuration_keys.get("ResetRetries"):
        try:
            ws_url = f"{settings.ws_url}{settings.station_id}"
            logger.info(f"Tentando conectar a {ws_url}")

            ws = await asyncio.wait_for(
                websockets.connect(
                    ws_url,
                    subprotocols=["ocpp2.0.1"],
                    ping_interval=20,
                    ping_timeout=10
                ),
                timeout=configuration_keys.get("ConnectionTimeOut")
            )

            async with ws:
                cp = ChargePoint(settings.station_id, ws, response_timeout=settings.response_timeout)

                # Inicia o loop de recebimento de mensagens em background
                start_task = asyncio.create_task(cp.start())
                heartbeat_task = None
                '''periodic_meter_values_task = None'''

                try:
                    # Envia BootNotification e aguarda aceitação
                    if not await cp.send_boot_notification():
                        logger.error("BootNotification nao aceito, desconectando...")
                        start_task.cancel()
                        try:
                            await start_task
                        except asyncio.CancelledError:
                            pass
                        continue  # Tenta reconectar
                        
                        
                    # Envia StatusNotification para o CP
                    '''
                    await cp.send_status_notification(
                        connector_id=0, 
                        status=state.status,
                        error_code=state.error_code
                    )
                    '''
                    
                    # Inicializa os conectores na store global
                    state.initialize_connectors(settings.evse_qty, settings.connectors_qty)

                    # Envia StatusNotification para cada conector
                    for evse in range(1, settings.evse_qty + 1):
                        for connectorId in range(1, settings.connectors_qty + 1):
                            connector = state.get_connector_state(evse, connectorId)
                                
                            await cp.send_status_notification(
                                evse_id=connector.get("evse_id"),
                                connector_id=connector.get("connector_id"),
                                status=connector.get("status"),
                            )
                    

                    # Notifica conexão estabelecida
                    if on_connect:
                        await on_connect(cp)
                    
                    # --- Tarefa 2: heartbeat periódico (iniciado APÓS BootNotification) ---
                    heartbeat_task = asyncio.create_task(cp.send_heartbeat())
                    
                    '''
                    # --- Tarefa 3: meter values periódicos ---
                    periodic_meter_values_task = asyncio.create_task(cp.send_periodic_meter_values())
                    '''

                    # Aguarda a primeira tarefa finalizar (conexão perdida ou heartbeat falhou)
                    done, pending = await asyncio.wait(
                        # [start_task, heartbeat_task, periodic_meter_values_task],
                        [start_task, heartbeat_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # Cancela a tarefa que ainda está pendente
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

                    # Verifica se alguma das tarefas concluídas lançou exceção
                    for task in done:
                        exc = task.exception()
                        if exc and not isinstance(exc, asyncio.CancelledError):
                            logger.error(f"Tarefa finalizou com exceção: {exc}")
                            # Relança a exceção para que o loop externo trate como falha
                            raise exc
                        
                    # Cancela o processador de comandos
                    if cp._command_processor_task and not cp._command_processor_task.done():
                        await cp._command_queue.put(None)  # sinal de parada
                        cp._command_processor_task.cancel()
                        try:
                            await cp._command_processor_task
                        except asyncio.CancelledError:
                            pass
                    
                    # Cancelar tasks de recarga
                    await cp.remote_handler.cancel_all_recharge_tasks()
                            
                except asyncio.CancelledError:
                    logger.info("Cancelamento detectado, encerrando tarefas internas...")
                    '''for task in (start_task, heartbeat_task, periodic_meter_values_task):'''
                    for task in (start_task, heartbeat_task):
                        if task and not task.done():
                            task.cancel()
                    await asyncio.gather(
                        '''*[t for t in (start_task, heartbeat_task, periodic_meter_values_task) if t],'''
                        *[t for t in (start_task, heartbeat_task) if t],
                        return_exceptions=True
                    )
                    raise


                except Exception as e:
                    '''for task in (start_task, heartbeat_task, periodic_meter_values_task):'''
                    for task in (start_task, heartbeat_task):
                        if task and not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                    logger.error(f"Erro durante a operacao: {e}")
                    raise

                # Se chegou aqui, a conexão foi encerrada voluntariamente (raro)
                logger.info("Conexao encerrada normalmente.")
                break

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError, asyncio.TimeoutError) as e:
            retries += 1
            delay = settings.base_delay * 1
            logger.warning(f"Conexao perdida ou falhou. Tentativa {retries}/{settings.reset_retries}. "
                           f"Reconectando em {delay:.1f}s. Erro: {e}")
            if on_disconnect:
                await on_disconnect()
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.info("Tarefa de conexao cancelada.")
            if on_disconnect:
                await on_disconnect()
            break
        except Exception as e:
            logger.exception(f"Erro inesperado: {e}")
            if on_disconnect:
                await on_disconnect()
            break
    else:
        logger.error("Numero maximo de tentativas atingido. Encerrando.")
        if on_disconnect:
            await on_disconnect()