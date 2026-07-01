# store/state.py
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict
import uuid

from ocpp.v201 import enums
from ocpp.v201 import datatypes 

from .base import BaseLockedState, locked
    
@dataclass
class ConnectorState:
    """Estado de um conector individual."""
    evse_id: int 
    connector_id: int
    status: enums.ConnectorStatusEnumType = enums.ConnectorStatusEnumType.available
    transaction_id: str | None = None
    transaction_seq_no: int = 0
    timestamp: datetime | None = None


@dataclass
class ChargePointState(BaseLockedState):
    """
    Estado global do Charge Point, incluindo status, configurações, transações.
    """
    def __init__(self):
        super().__init__()
        # Status básico do CP
        self.registration: enums.ConnectorStatusEnumType | None = None
        self.status: enums.ConnectorStatusEnumType = enums.ConnectorStatusEnumType.available
    
        # Conectores
        self.evse_qty: int = 0
        self.connectors_qty: int = 0
        self.connectors: List[List[ConnectorState]] = []  
    
        # Relógio Interno para sincronização com o servidor
        self.server_current_time: str | None = None
        self.time_offset: float = 0.0
    
        # Lista de autorizações locais (local auth list)
        """       
        !!!! A IMPLEMENTAR !!!!
        
        self.local_auth_list: List[datatypes.AuthorizationData] = = []
        self.local_auth_list_version: int = 0
        """

        # Perfis de carregamento ativos (TxProfile, ChargePointMaxProfile, etc.)
        """       
        !!!! A IMPLEMENTAR !!!!
        self.charging_profiles: List[datatypes.ChargingProfile] = = []
        """

        # Transação atual (se houver)
        self.current_transaction_id: int | None = None
        self.current_connector_id: int | None = None
        self.id_tag_in_transaction: str | None = None
        self.current_transaction_power: float | None = None

        
        # Reservas ativas
        """
        !!!! A IMPLEMENTAR !!!!
        self.reservations: List[dict] = = []  # Você pode criar uma dataclass 
        """

    @locked
    def initialize_connectors(self, evse_qty: int, connectors_qty: int):
        """
        Inicializa a lista de conectores com estado padrão (Available).
        Deve ser chamado após a leitura da configuração e antes do primeiro StatusNotification.
        Os IDs dos conectores serão de 1 a qty.
        """
        self.connectors.clear()
        self.connectors_qty = connectors_qty
        self.evse_qty = evse_qty
           
        for i in range(1, evse_qty + 1):
            evse = []
            for j in range(1, connectors_qty + 1):
                evse.append(ConnectorState(evse_id=i, connector_id=j))
            self.connectors.append(evse)

    @locked
    def update_connector_status(
        self,
        evse_id: int,
        connector_id: int,
        status: enums.ConnectorStatusEnumType,
    ):
        """
        Atualiza o estado de um conector específico.
        Se o conector não existir, levanta ValueError.
        """
        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                conn.status = status
                conn.timestamp = self.get_current_time()
                return
        raise ValueError(f"Connector {connector_id} não encontrado")
    

    @locked
    def create_connector_transactionId(
        self,
        evse_id: int,
        connector_id: int,
    )  -> datetime :

        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                conn.transaction_id = str(uuid.uuid4())
                conn.timestamp = self.get_current_time()
                conn.transaction_seq_no = 0
                return conn.transaction_id
        raise ValueError(f"Connector {connector_id} não encontrado")
    
    @locked
    def set_connector_transactionId(
        self,
        evse_id: int,
        connector_id: int,
        remote_start_id: int
    )  -> datetime :

        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                conn.transaction_id = str(remote_start_id)
                conn.timestamp = self.get_current_time()
                conn.transaction_seq_no = 0
                return conn.transaction_id
        raise ValueError(f"Connector {connector_id} não encontrado")
    
    @locked
    def clear_connector_transactionId(
        self,
        evse_id: int,
        connector_id: int,
    ):

        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                conn.transaction_id = None
                conn.transaction_seq_no = 0
                conn.timestamp = self.get_current_time()
                return
        raise ValueError(f"Connector {connector_id} não encontrado")
    
    @locked
    def update_transaction_seq_no(
        self,
        evse_id: int,
        connector_id: int,
    ):
        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                conn.transaction_seq_no += 1
                return
        raise ValueError(f"Connector {connector_id} não encontrado")
    
    def is_transaction_active(self, transaction_id: int, evse_id: Optional[int] = 1) -> int | None:
        evse = self.connectors[evse_id - 1]

        for connector in evse:
            if connector.transaction_id == transaction_id:
                return connector.connector_id
        return None
       
    def get_connector_state(self, evse_id: int, connector_id: int) -> dict | None:
        """
        Retorna um dicionario contendo as propriedades do ConnectorState com o ID fornecido, ou None se não existir.
        """
        evse = self.connectors[evse_id - 1]  

        for conn in evse:
            if conn.connector_id == connector_id:
                return {
                    'evse_id': conn.evse_id,
                    'connector_id': conn.connector_id,
                    'status': conn.status,
                    'transaction_id': conn.transaction_id,
                    'transaction_seq_no': conn.transaction_seq_no,
                    'timestamp': conn.timestamp,
                }
        raise ValueError(f"Connector {connector_id} não encontrado")
    
        
    @locked
    def update_time_from_server(self, server_time_iso: str):
        """
        Método que garante a sincronicidade entre o relógio do servidor e o relógio virtual do cliente ocpp
        """
        server_time = datetime.fromisoformat(server_time_iso.replace('Z', '+00:00'))
        local_time = datetime.now(timezone.utc)
        self.time_offset = (server_time - local_time).total_seconds()
        self.server_current_time = server_time_iso
    
    def get_current_time(self) -> datetime:
        """Retorna a hora atual ajustada pelo offset (se desejado)."""
        ts = datetime.now(timezone.utc) + timedelta(seconds=self.time_offset)
        iso_ts = ts.isoformat(timespec='milliseconds').replace("+00:00", "Z")

        return iso_ts


# Criamos uma única instância global deste estado
state = ChargePointState()
