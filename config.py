#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Конфигурация улучшенного фандинг-бота
"""

import os
from dataclasses import dataclass
from typing import Dict, List
import pytz

@dataclass
class BotConfig:
    """Основная конфигурация бота"""
    telegram_token: str = os.getenv('________________________')
    admin_chat_id: int = int(os.getenv('ADMIN_CHAT_ID', '______________'))
    
    # База данных
    database_path: str = 'funding_bot_v2.db'
    
    # Часовой пояс (Киев)
    default_timezone: str = 'Europe/Kiev'
    
    # Мониторинг
    min_apr_threshold: float = 50.0
    alert_apr_threshold: float = 100.0
    update_interval: int = 30  # секунд
    max_websocket_connections: int = 5
    reconnect_delay: int = 5
    
    # Данные
    data_retention_days: int = 7
    max_opportunities_display: int = 15
    
    # Парсинг
    request_timeout: int = 30
    max_retries: int = 3
    delay_between_requests: float = 1.0

@dataclass 
class WebSocketConfig:
    """Конфигурация WebSocket подключений"""
    
    # Endpoints для реал-тайм данных
    binance_futures: str = 'wss://fstream.binance.com/ws/'
    bybit_linear: str = 'wss://stream.bybit.com/v5/public/linear'
    okx_public: str = 'wss://ws.okx.com:8443/ws/v5/public'
    gateio_futures: str = 'wss://api.gateio.ws/ws/v4/'
    bitget_mix: str = 'wss://ws.bitget.com/mix/v1/stream'
    
    # Настройки подключения
    ping_interval: int = 20
    ping_timeout: int = 10
    close_timeout: int = 10
    max_size: int = 2**20  # 1MB
    
    # Retry параметры
    max_reconnects: int = 10
    reconnect_interval: int = 5

@dataclass
class ParsingConfig:
    """Конфигурация парсинга множественных источников"""
    
    # HTTP источники
    http_sources: List[str] = None
    
    # WebSocket источники  
    websocket_sources: Dict[str, str] = None
    
    # API источники (если доступны)
    api_sources: Dict[str, str] = None
    
    def __post_init__(self):
        if self.http_sources is None:
            self.http_sources = [
                'https://www.coinglass.com/FundingRate',
                'https://www.coinglass.com/en/FundingRate',
                'https://coinglass.com/api/fundingRate/v2/home',
                'https://www.binance.com/en/futures/funding-history',
                'https://www.bybit.com/en-US/help-center/bybitHC_Article',
                'https://coinmarketcap.com/rankings/exchanges/derivatives/',
                'https://www.okx.com/markets/prices',
                'https://www.gate.io/trade/BTC_USDT',
                'https://www.bitget.com/futures/BTCUSDT'
            ]
        
        if self.websocket_sources is None:
            self.websocket_sources = {
                'binance': 'wss://fstream.binance.com/ws/!ticker@arr',
                'bybit': 'wss://stream.bybit.com/v5/public/linear',
                'okx': 'wss://ws.okx.com:8443/ws/v5/public',
                'gateio': 'wss://api.gateio.ws/ws/v4/',
                'bitget': 'wss://ws.bitget.com/mix/v1/stream'
            }
        
        if self.api_sources is None:
            self.api_sources = {
                'binance_api': 'https://fapi.binance.com/fapi/v1/premiumIndex',
                'bybit_api': 'https://api.bybit.com/v2/public/symbols',
                'okx_api': 'https://www.okx.com/api/v5/public/funding-rate',
                'gateio_api': 'https://api.gateio.ws/api/v4/futures/usdt/contracts'
            }

# Глобальные экземпляры конфигурации
bot_config = BotConfig()
websocket_config = WebSocketConfig()
parsing_config = ParsingConfig()

# Топ-50 исключаемых монет
TOP_50_COINS = {
    'BTC', 'ETH', 'USDT', 'XRP', 'BNB', 'SOL', 'USDC', 'TRX', 
    'DOGE', 'ADA', 'WBTC', 'SUI', 'LINK', 'BCH', 'LEO', 'XLM',
    'AVAX', 'TON', 'WBT', 'SHIB', 'HBAR', 'LTC', 'XMR', 'DOT',
    'DAI', 'BGB', 'UNI', 'PEPE', 'AAVE', 'APT', 'TAO', 'OKB',
    'NEAR', 'ICP', 'CRO', 'ETC', 'ONDO', 'MNT', 'TKX', 'GT',
    'KAS', 'FTN', 'POL', 'VET', 'TRUMP', 'RENDER', 'SEI', 'ENA', 'FET'
}

# User agents для парсинга
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
]
