#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Реал-тайм мониторинг фандинг-рейтов через WebSocket
"""

import asyncio
import websockets
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
import pytz
from dataclasses import dataclass
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

from config import bot_config, websocket_config, parsing_config, TOP_50_COINS

logger = logging.getLogger(__name__)

@dataclass
class FundingUpdate:
    """Структура обновления фандинг-рейта"""
    symbol: str
    funding_rate: float
    apr: float
    exchange: str
    timestamp: datetime
    change_percent: float = 0.0
    
    def __post_init__(self):
        """Валидация и расчет APR"""
        if not self.symbol or len(self.symbol) < 2:
            raise ValueError("Некорректный символ")
        
        if abs(self.funding_rate) > 50:
            raise ValueError("Подозрительно высокий фандинг-рейт")
        
        # Автоматический расчет APR если не задан
        if self.apr == 0:
            self.apr = abs(self.funding_rate) * 3 * 365

class RealTimeMonitor:
    """Класс для реал-тайм мониторинга фандинг-рейтов"""
    
    def __init__(self, update_callback: Optional[Callable] = None):
        self.update_callback = update_callback
        self.connections: Dict[str, websockets.WebSocketServerProtocol] = {}
        self.is_running = False
        self.last_funding_data: Dict[str, FundingUpdate] = {}
        self.update_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # Киевский часовой пояс
        self.kiev_tz = pytz.timezone('Europe/Kiev')
        
        # Статистика
        self.stats = {
            'total_updates': 0,
            'alerts_sent': 0,
            'connections_lost': 0,
            'last_update': None
        }
    
    async def start_monitoring(self):
        """Запуск мониторинга всех источников"""
        logger.info("🚀 Запуск реал-тайм мониторинга фандинг-рейтов")
        self.is_running = True
        
        # Создание задач для каждой биржи
        tasks = []
        
        for exchange, endpoint in parsing_config.websocket_sources.items():
            task = asyncio.create_task(
                self.monitor_exchange(exchange, endpoint),
                name=f"monitor_{exchange}"
            )
            tasks.append(task)
        
        # Задача обработки обновлений
        processing_task = asyncio.create_task(
            self.process_updates(),
            name="process_updates"
        )
        tasks.append(processing_task)
        
        # Задача периодической очистки
        cleanup_task = asyncio.create_task(
            self.periodic_cleanup(),
            name="periodic_cleanup"
        )
        tasks.append(cleanup_task)
        
        try:
            # Ожидание завершения всех задач
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Ошибка в мониторинге: {e}")
        finally:
            await self.stop_monitoring()
    
    async def monitor_exchange(self, exchange: str, endpoint: str):
        """Мониторинг конкретной биржи"""
        logger.info(f"📡 Подключение к {exchange}: {endpoint}")
        
        reconnect_count = 0
        max_reconnects = websocket_config.max_reconnects
        
        while self.is_running and reconnect_count < max_reconnects:
            try:
                async with websockets.connect(
                    endpoint,
                    ping_interval=websocket_config.ping_interval,
                    ping_timeout=websocket_config.ping_timeout,
                    close_timeout=websocket_config.close_timeout,
                    max_size=websocket_config.max_size
                ) as websocket:
                    
                    self.connections[exchange] = websocket
                    logger.info(f"✅ Подключено к {exchange}")
                    
                    # Подписка на фандинг-рейты
                    await self.subscribe_funding_rates(websocket, exchange)
                    
                    reconnect_count = 0  # Сброс счетчика при успешном подключении
                    
                    # Основной цикл получения данных
                    async for message in websocket:
                        try:
                            await self.process_websocket_message(message, exchange)
                        except Exception as e:
                            logger.error(f"Ошибка обработки сообщения {exchange}: {e}")
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"⚠️ Соединение с {exchange} закрыто")
                reconnect_count += 1
                self.stats['connections_lost'] += 1
                
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к {exchange}: {e}")
                reconnect_count += 1
            
            if self.is_running and reconnect_count < max_reconnects:
                logger.info(f"🔄 Переподключение к {exchange} через {websocket_config.reconnect_interval}с...")
                await asyncio.sleep(websocket_config.reconnect_interval)
        
        if reconnect_count >= max_reconnects:
            logger.error(f"💀 Превышено количество попыток переподключения к {exchange}")
        
        # Удаление из активных подключений
        self.connections.pop(exchange, None)
    
    async def subscribe_funding_rates(self, websocket, exchange: str):
        """Подписка на фандинг-рейты биржи"""
        try:
            if exchange == 'binance':
                # Binance: подписка на все тикеры
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": ["!ticker@arr"],
                    "id": 1
                }
                
            elif exchange == 'bybit':
                # Bybit: подписка на фандинг-рейты
                subscribe_msg = {
                    "op": "subscribe",
                    "args": ["tickers.linear"]
                }
                
            elif exchange == 'okx':
                # OKX: подписка на фандинг-рейты
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [{"channel": "funding-rate", "instType": "SWAP"}]
                }
                
            elif exchange == 'gateio':
                # Gate.io: подписка на фандинг-рейты
                subscribe_msg = {
                    "method": "futures.funding_rate",
                    "params": [],
                    "id": 1
                }
                
            elif exchange == 'bitget':
                # Bitget: подписка на фандинг-рейты
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [{"instType": "mc", "channel": "funding-rate", "instId": "default"}]
                }
            
            else:
                logger.warning(f"Неизвестная биржа для подписки: {exchange}")
                return
            
            await websocket.send(json.dumps(subscribe_msg))
            logger.info(f"📋 Подписка на фандинг-рейты {exchange} отправлена")
            
        except Exception as e:
            logger.error(f"Ошибка подписки на {exchange}: {e}")
    
    async def process_websocket_message(self, message: str, exchange: str):
        """Обработка WebSocket сообщения"""
        try:
            data = json.loads(message)
            
            # Извлечение фандинг-данных в зависимости от биржи
            funding_updates = []
            
            if exchange == 'binance':
                funding_updates = self.parse_binance_data(data)
            elif exchange == 'bybit':
                funding_updates = self.parse_bybit_data(data)
            elif exchange == 'okx':
                funding_updates = self.parse_okx_data(data)
            elif exchange == 'gateio':
                funding_updates = self.parse_gateio_data(data)
            elif exchange == 'bitget':
                funding_updates = self.parse_bitget_data(data)
            
            # Добавление обновлений в очередь
            for update in funding_updates:
                if self.should_process_update(update):
                    self.update_queue.put(update)
                    self.stats['total_updates'] += 1
            
        except json.JSONDecodeError:
            logger.debug(f"Невалидный JSON от {exchange}")
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения {exchange}: {e}")
    
    def parse_binance_data(self, data: dict) -> List[FundingUpdate]:
        """Парсинг данных Binance"""
        updates = []
        
        try:
            # Binance отправляет массив тикеров
            if isinstance(data, list):
                for ticker in data:
                    symbol = ticker.get('s', '').replace('USDT', '').replace('USD', '')
                    
                    if symbol in TOP_50_COINS:
                        continue
                    
                    # Получение фандинг-рейта (если доступен)
                    funding_rate = float(ticker.get('r', 0))  # 'r' - funding rate
                    
                    if funding_rate != 0:
                        update = FundingUpdate(
                            symbol=symbol,
                            funding_rate=funding_rate * 100,  # Конвертация в проценты
                            apr=0,  # Будет рассчитан автоматически
                            exchange='Binance',
                            timestamp=datetime.now(self.kiev_tz)
                        )
                        updates.append(update)
                        
        except Exception as e:
            logger.debug(f"Ошибка парсинга Binance данных: {e}")
        
        return updates
    
    def parse_bybit_data(self, data: dict) -> List[FundingUpdate]:
        """Парсинг данных Bybit"""
        updates = []
        
        try:
            # Bybit: структура данных
            if 'data' in data and isinstance(data['data'], list):
                for item in data['data']:
                    symbol = item.get('symbol', '').replace('USDT', '').replace('USD', '')
                    
                    if symbol in TOP_50_COINS:
                        continue
                    
                    funding_rate = float(item.get('fundingRate', 0))
                    
                    if funding_rate != 0:
                        update = FundingUpdate(
                            symbol=symbol,
                            funding_rate=funding_rate * 100,
                            apr=0,
                            exchange='Bybit',
                            timestamp=datetime.now(self.kiev_tz)
                        )
                        updates.append(update)
                        
        except Exception as e:
            logger.debug(f"Ошибка парсинга Bybit данных: {e}")
        
        return updates
    
    def parse_okx_data(self, data: dict) -> List[FundingUpdate]:
        """Парсинг данных OKX"""
        updates = []
        
        try:
            if 'data' in data and isinstance(data['data'], list):
                for item in data['data']:
                    symbol = item.get('instId', '').replace('-USDT-SWAP', '').replace('-USD-SWAP', '')
                    
                    if symbol in TOP_50_COINS:
                        continue
                    
                    funding_rate = float(item.get('fundingRate', 0))
                    
                    if funding_rate != 0:
                        update = FundingUpdate(
                            symbol=symbol,
                            funding_rate=funding_rate * 100,
                            apr=0,
                            exchange='OKX',
                            timestamp=datetime.now(self.kiev_tz)
                        )
                        updates.append(update)
                        
        except Exception as e:
            logger.debug(f"Ошибка парсинга OKX данных: {e}")
        
        return updates
    
    def parse_gateio_data(self, data: dict) -> List[FundingUpdate]:
        """Парсинг данных Gate.io"""
        updates = []
        
        try:
            if 'result' in data and isinstance(data['result'], list):
                for item in data['result']:
                    symbol = item.get('contract', '').replace('_USDT', '').replace('_USD', '')
                    
                    if symbol in TOP_50_COINS:
                        continue
                    
                    funding_rate = float(item.get('funding_rate', 0))
                    
                    if funding_rate != 0:
                        update = FundingUpdate(
                            symbol=symbol,
                            funding_rate=funding_rate * 100,
                            apr=0,
                            exchange='Gate.io',
                            timestamp=datetime.now(self.kiev_tz)
                        )
                        updates.append(update)
                        
        except Exception as e:
            logger.debug(f"Ошибка парсинга Gate.io данных: {e}")
        
        return updates
    
    def parse_bitget_data(self, data: dict) -> List[FundingUpdate]:
        """Парсинг данных Bitget"""
        updates = []
        
        try:
            if 'data' in data and isinstance(data['data'], list):
                for item in data['data']:
                    symbol = item.get('instId', '').replace('USDT', '').replace('USD', '')
                    
                    if symbol in TOP_50_COINS:
                        continue
                    
                    funding_rate = float(item.get('fundingRate', 0))
                    
                    if funding_rate != 0:
                        update = FundingUpdate(
                            symbol=symbol,
                            funding_rate=funding_rate * 100,
                            apr=0,
                            exchange='Bitget',
                            timestamp=datetime.now(self.kiev_tz)
                        )
                        updates.append(update)
                        
        except Exception as e:
            logger.debug(f"Ошибка парсинга Bitget данных: {e}")
        
        return updates
    
    def should_process_update(self, update: FundingUpdate) -> bool:
        """Определение необходимости обработки обновления"""
        try:
            # Проверка APR порога
            if update.apr < bot_config.min_apr_threshold:
                return False
            
            # Проверка на дубликат
            key = f"{update.symbol}_{update.exchange}"
            
            if key in self.last_funding_data:
                last_update = self.last_funding_data[key]
                
                # Проверка времени последнего обновления (не чаще чем раз в минуту)
                time_diff = update.timestamp - last_update.timestamp
                if time_diff.total_seconds() < 60:
                    return False
                
                # Расчет изменения
                change = abs(update.funding_rate - last_update.funding_rate)
                change_percent = (change / abs(last_update.funding_rate)) * 100 if last_update.funding_rate != 0 else 0
                update.change_percent = change_percent
                
                # Обработка только значительных изменений (>10%)
                if change_percent < 10:
                    return False
            
            # Сохранение последнего обновления
            self.last_funding_data[key] = update
            return True
            
        except Exception as e:
            logger.error(f"Ошибка проверки обновления: {e}")
            return False
    
    async def process_updates(self):
        """Обработка очереди обновлений"""
        logger.info("📋 Запуск обработчика обновлений")
        
        while self.is_running:
            try:
                # Получение обновлений из очереди
                updates_batch = []
                
                # Собираем пакет обновлений (максимум 10 за раз)
                for _ in range(10):
                    try:
                        update = self.update_queue.get_nowait()
                        updates_batch.append(update)
                    except queue.Empty:
                        break
                
                if updates_batch:
                    await self.process_funding_updates(updates_batch)
                
                # Ожидание перед следующей обработкой
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Ошибка обработки очереди обновлений: {e}")
                await asyncio.sleep(5)
    
    async def process_funding_updates(self, updates: List[FundingUpdate]):
        """Обработка пакета обновлений фандинга"""
        try:
            high_apr_updates = []
            
            for update in updates:
                # Фильтрация высокодоходных возможностей
                if update.apr >= bot_config.alert_apr_threshold:
                    high_apr_updates.append(update)
                    logger.info(f"🔥 Высокий APR: {update.symbol} - {update.apr}% ({update.exchange})")
            
            # Отправка алертов если есть callback
            if high_apr_updates and self.update_callback:
                await self.send_alerts(high_apr_updates)
            
            # Обновление статистики
            self.stats['last_update'] = datetime.now(self.kiev_tz)
            
        except Exception as e:
            logger.error(f"Ошибка обработки обновлений фандинга: {e}")
    
    async def send_alerts(self, updates: List[FundingUpdate]):
        """Отправка алертов о высокодоходных возможностях"""
        try:
            if self.update_callback:
                await self.update_callback(updates)
                self.stats['alerts_sent'] += len(updates)
                logger.info(f"📬 Отправлено {len(updates)} алертов")
        except Exception as e:
            logger.error(f"Ошибка отправки алертов: {e}")
    
    async def periodic_cleanup(self):
        """Периодическая очистка старых данных"""
        logger.info("🧹 Запуск периодической очистки")
        
        while self.is_running:
            try:
                # Очистка старых данных (старше 1 часа)
                cutoff_time = datetime.now(self.kiev_tz) - timedelta(hours=1)
                
                keys_to_remove = []
                for key, update in self.last_funding_data.items():
                    if update.timestamp < cutoff_time:
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del self.last_funding_data[key]
                
                if keys_to_remove:
                    logger.info(f"🗑️ Очищено {len(keys_to_remove)} старых записей")
                
                # Ожидание 30 минут до следующей очистки
                await asyncio.sleep(1800)
                
            except Exception as e:
                logger.error(f"Ошибка периодической очистки: {e}")
                await asyncio.sleep(300)  # 5 минут при ошибке
    
    async def stop_monitoring(self):
        """Остановка мониторинга"""
        logger.info("🛑 Остановка реал-тайм мониторинга")
        self.is_running = False
        
        # Закрытие всех WebSocket подключений
        for exchange, websocket in self.connections.items():
            try:
                await websocket.close()
                logger.info(f"🔌 Закрыто подключение к {exchange}")
            except Exception as e:
                logger.error(f"Ошибка закрытия {exchange}: {e}")
        
        self.connections.clear()
        
        # Остановка executor
        self.executor.shutdown(wait=True)
        
        logger.info("✅ Мониторинг остановлен")
    
    def get_stats(self) -> Dict:
        """Получение статистики мониторинга"""
        return {
            **self.stats,
            'active_connections': len(self.connections),
            'queue_size': self.update_queue.qsize(),
            'tracked_symbols': len(self.last_funding_data),
            'uptime': datetime.now(self.kiev_tz).isoformat()
        }

# Фабрика для создания монитора
def create_real_time_monitor(update_callback: Optional[Callable] = None) -> RealTimeMonitor:
    """Создание экземпляра реал-тайм монитора"""
    return RealTimeMonitor(update_callback)
