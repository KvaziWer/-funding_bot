#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Расширенная база данных для фандинг-бота
"""

import sqlite3
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import pytz
from contextlib import contextmanager

from config import bot_config

logger = logging.getLogger(__name__)

@dataclass
class UserProfile:
    """Профиль пользователя"""
    user_id: int
    deposit: float
    min_apr: float
    timezone: str
    notifications: bool
    auto_alerts: bool
    max_position_size: float
    created_at: datetime
    last_active: datetime
    
class DatabaseManager:
    """Расширенный менеджер базы данных"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or bot_config.database_path
        self.kiev_tz = pytz.timezone('Europe/Kiev')
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для соединения с БД"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row  # Для доступа по имени колонки
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка базы данных: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def init_database(self):
        """Инициализация расширенной схемы БД"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Таблица пользователей (расширенная)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        deposit REAL DEFAULT 1000,
                        min_apr REAL DEFAULT 50,
                        timezone TEXT DEFAULT 'Europe/Kiev',
                        notifications BOOLEAN DEFAULT 1,
                        auto_alerts BOOLEAN DEFAULT 1,
                        max_position_size REAL DEFAULT 0.8,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        settings TEXT DEFAULT '{}'
                    )
                ''')
                
                # Таблица истории фандинг-рейтов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS funding_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        funding_rate REAL NOT NULL,
                        apr REAL NOT NULL,
                        exchange TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        source TEXT DEFAULT 'websocket',
                        change_percent REAL DEFAULT 0
                    )
                ''')
                
                # Таблица алертов
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS alerts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        symbol TEXT NOT NULL,
                        funding_rate REAL NOT NULL,
                        apr REAL NOT NULL,
                        exchange TEXT NOT NULL,
                        alert_type TEXT DEFAULT 'high_apr',
                        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        read_at TIMESTAMP NULL,
                        FOREIGN KEY (user_id) REFERENCES users (user_id)
                    )
                ''')
                
                # Таблица статистики пользователей
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_stats (
                        user_id INTEGER PRIMARY KEY,
                        total_scans INTEGER DEFAULT 0,
                        alerts_received INTEGER DEFAULT 0,
                        best_apr_found REAL DEFAULT 0,
                        total_profit_calculated REAL DEFAULT 0,
                        last_profit_calculation TIMESTAMP NULL,
                        FOREIGN KEY (user_id) REFERENCES users (user_id)
                    )
                ''')
                
                # Таблица системной статистики
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS system_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        metric_name TEXT NOT NULL,
                        metric_value REAL NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Индексы для оптимизации
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_funding_symbol_timestamp ON funding_history(symbol, timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_alerts_user_timestamp ON alerts(user_id, sent_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_funding_apr ON funding_history(apr)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(last_active)')
                
                conn.commit()
                logger.info("✅ База данных инициализирована успешно")
                
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            raise
    
    # Работа с пользователями
    def get_user_profile(self, user_id: int) -> Optional[UserProfile]:
        """Получение полного профиля пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT user_id, deposit, min_apr, timezone, notifications, 
                           auto_alerts, max_position_size, created_at, last_active
                    FROM users WHERE user_id = ?
                ''', (user_id,))
                
                row = cursor.fetchone()
                if row:
                    return UserProfile(
                        user_id=row['user_id'],
                        deposit=row['deposit'],
                        min_apr=row['min_apr'],
                        timezone=row['timezone'],
                        notifications=bool(row['notifications']),
                        auto_alerts=bool(row['auto_alerts']),
                        max_position_size=row['max_position_size'],
                        created_at=datetime.fromisoformat(row['created_at']),
                        last_active=datetime.fromisoformat(row['last_active'])
                    )
                return None
        except Exception as e:
            logger.error(f"Ошибка получения профиля пользователя {user_id}: {e}")
            return None
    
    def create_or_update_user(self, user_id: int, **kwargs) -> bool:
        """Создание или обновление пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Проверка существования пользователя
                cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
                exists = cursor.fetchone() is not None
                
                current_time = datetime.now(self.kiev_tz).isoformat()
                
                if exists:
                    # Обновление существующего пользователя
                    update_fields = []
                    update_values = []
                    
                    for field, value in kwargs.items():
                        if field in ['deposit', 'min_apr', 'timezone', 'notifications', 
                                   'auto_alerts', 'max_position_size']:
                            update_fields.append(f"{field} = ?")
                            update_values.append(value)
                    
                    if update_fields:
                        update_fields.append("last_active = ?")
                        update_values.append(current_time)
                        update_values.append(user_id)
                        
                        query = f"UPDATE users SET {', '.join(update_fields)} WHERE user_id = ?"
                        cursor.execute(query, update_values)
                else:
                    # Создание нового пользователя
                    cursor.execute('''
                        INSERT INTO users (user_id, deposit, min_apr, timezone, notifications, 
                                         auto_alerts, max_position_size, created_at, last_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        user_id,
                        kwargs.get('deposit', 1000),
                        kwargs.get('min_apr', 50),
                        kwargs.get('timezone', 'Europe/Kiev'),
                        kwargs.get('notifications', True),
                        kwargs.get('auto_alerts', True),
                        kwargs.get('max_position_size', 0.8),
                        current_time,
                        current_time
                    ))
                    
                    # Создание записи статистики
                    cursor.execute('''
                        INSERT INTO user_stats (user_id) VALUES (?)
                    ''', (user_id,))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка создания/обновления пользователя {user_id}: {e}")
            return False
    
    def update_user_activity(self, user_id: int):
        """Обновление времени последней активности"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE users SET last_active = ? WHERE user_id = ?
                ''', (datetime.now(self.kiev_tz).isoformat(), user_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления активности пользователя {user_id}: {e}")
    
    # Работа с фандинг-данными
    def save_funding_updates(self, updates: List) -> bool:
        """Сохранение пакета обновлений фандинга"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for update in updates:
                    cursor.execute('''
                        INSERT INTO funding_history 
                        (symbol, funding_rate, apr, exchange, timestamp, source, change_percent)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        update.symbol,
                        update.funding_rate,
                        update.apr,
                        update.exchange,
                        update.timestamp.isoformat(),
                        'websocket',
                        getattr(update, 'change_percent', 0)
                    ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения фандинг-обновлений: {e}")
            return False
    
    def get_funding_history(self, symbol: str = None, hours: int = 24) -> List[Dict]:
        """Получение истории фандинг-рейтов"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now(self.kiev_tz) - timedelta(hours=hours)
                
                if symbol:
                    cursor.execute('''
                        SELECT * FROM funding_history 
                        WHERE symbol = ? AND timestamp > ?
                        ORDER BY timestamp DESC
                    ''', (symbol, cutoff_time.isoformat()))
                else:
                    cursor.execute('''
                        SELECT * FROM funding_history 
                        WHERE timestamp > ?
                        ORDER BY timestamp DESC
                        LIMIT 1000
                    ''', (cutoff_time.isoformat(),))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Ошибка получения истории фандинга: {e}")
            return []
    
    def get_top_opportunities(self, limit: int = 10) -> List[Dict]:
        """Получение топ возможностей за последний час"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now(self.kiev_tz) - timedelta(hours=1)
                
                cursor.execute('''
                    SELECT symbol, funding_rate, apr, exchange, timestamp,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                    FROM funding_history 
                    WHERE timestamp > ? AND apr >= ?
                    ORDER BY apr DESC
                ''', (cutoff_time.isoformat(), bot_config.min_apr_threshold))
                
                # Фильтрация только последних записей для каждого символа
                all_rows = cursor.fetchall()
                unique_symbols = {}
                
                for row in all_rows:
                    if row['rn'] == 1:  # Только последние записи
                        unique_symbols[row['symbol']] = dict(row)
                
                # Сортировка по APR и возврат топа
                sorted_opportunities = sorted(
                    unique_symbols.values(), 
                    key=lambda x: x['apr'], 
                    reverse=True
                )
                
                return sorted_opportunities[:limit]
                
        except Exception as e:
            logger.error(f"Ошибка получения топ возможностей: {e}")
            return []
    
    # Работа с алертами
    def save_alert(self, user_id: int, symbol: str, funding_rate: float, 
                  apr: float, exchange: str, alert_type: str = 'high_apr') -> bool:
        """Сохранение алерта"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO alerts (user_id, symbol, funding_rate, apr, exchange, alert_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, symbol, funding_rate, apr, exchange, alert_type))
                
                # Обновление статистики пользователя
                cursor.execute('''
                    UPDATE user_stats 
                    SET alerts_received = alerts_received + 1,
                        best_apr_found = MAX(best_apr_found, ?)
                    WHERE user_id = ?
                ''', (apr, user_id))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения алерта: {e}")
            return False
    
    def get_user_alerts(self, user_id: int, unread_only: bool = False, limit: int = 50) -> List[Dict]:
        """Получение алертов пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = '''
                    SELECT * FROM alerts 
                    WHERE user_id = ?
                '''
                params = [user_id]
                
                if unread_only:
                    query += ' AND read_at IS NULL'
                
                query += ' ORDER BY sent_at DESC LIMIT ?'
                params.append(limit)
                
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Ошибка получения алертов пользователя {user_id}: {e}")
            return []
    
    def mark_alerts_read(self, user_id: int, alert_ids: List[int] = None) -> bool:
        """Отметка алертов как прочитанных"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                current_time = datetime.now(self.kiev_tz).isoformat()
                
                if alert_ids:
                    placeholders = ','.join(['?'] * len(alert_ids))
                    cursor.execute(f'''
                        UPDATE alerts SET read_at = ? 
                        WHERE user_id = ? AND id IN ({placeholders})
                    ''', [current_time, user_id] + alert_ids)
                else:
                    cursor.execute('''
                        UPDATE alerts SET read_at = ? 
                        WHERE user_id = ? AND read_at IS NULL
                    ''', (current_time, user_id))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка отметки алертов как прочитанных: {e}")
            return False
    
    # Статистика
    def update_user_stats(self, user_id: int, **kwargs) -> bool:
        """Обновление статистики пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                update_fields = []
                update_values = []
                
                for field, value in kwargs.items():
                    if field in ['total_scans', 'alerts_received', 'best_apr_found', 
                               'total_profit_calculated', 'last_profit_calculation']:
                        if field == 'total_scans':
                            update_fields.append("total_scans = total_scans + ?")
                        elif field == 'alerts_received':
                            update_fields.append("alerts_received = alerts_received + ?")
                        elif field == 'total_profit_calculated':
                            update_fields.append("total_profit_calculated = total_profit_calculated + ?")
                        else:
                            update_fields.append(f"{field} = ?")
                        update_values.append(value)
                
                if update_fields:
                    update_values.append(user_id)
                    query = f"UPDATE user_stats SET {', '.join(update_fields)} WHERE user_id = ?"
                    cursor.execute(query, update_values)
                    conn.commit()
                    return True
                
        except Exception as e:
            logger.error(f"Ошибка обновления статистики пользователя {user_id}: {e}")
            return False
    
    def get_user_stats(self, user_id: int) -> Dict:
        """Получение статистики пользователя"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
                row = cursor.fetchone()
                
                if row:
                    return dict(row)
                else:
                    # Создание записи статистики если не существует
                    cursor.execute('INSERT INTO user_stats (user_id) VALUES (?)', (user_id,))
                    conn.commit()
                    return {
                        'user_id': user_id,
                        'total_scans': 0,
                        'alerts_received': 0,
                        'best_apr_found': 0,
                        'total_profit_calculated': 0,
                        'last_profit_calculation': None
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка получения статистики пользователя {user_id}: {e}")
            return {}
    
    def save_system_metric(self, metric_name: str, metric_value: float) -> bool:
        """Сохранение системной метрики"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO system_stats (metric_name, metric_value)
                    VALUES (?, ?)
                ''', (metric_name, metric_value))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка сохранения системной метрики: {e}")
            return False
    
    def cleanup_old_data(self, days: int = None) -> bool:
        """Очистка старых данных"""
        try:
            days = days or bot_config.data_retention_days
            cutoff_date = datetime.now(self.kiev_tz) - timedelta(days=days)
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Очистка старой истории фандинга
                cursor.execute('''
                    DELETE FROM funding_history 
                    WHERE timestamp < ?
                ''', (cutoff_date.isoformat(),))
                
                funding_deleted = cursor.rowcount
                
                # Очистка старых алертов
                cursor.execute('''
                    DELETE FROM alerts 
                    WHERE sent_at < ? AND read_at IS NOT NULL
                ''', (cutoff_date.isoformat(),))
                
                alerts_deleted = cursor.rowcount
                
                # Очистка старых системных метрик
                cursor.execute('''
                    DELETE FROM system_stats 
                    WHERE timestamp < ?
                ''', (cutoff_date.isoformat(),))
                
                metrics_deleted = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"🗑️ Очищено: {funding_deleted} записей фандинга, "
                          f"{alerts_deleted} алертов, {metrics_deleted} метрик")
                
                return True
                
        except Exception as e:
            logger.error(f"Ошибка очистки старых данных: {e}")
            return False
    
    def get_system_stats(self) -> Dict:
        """Получение системной статистики"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Общее количество пользователей
                cursor.execute('SELECT COUNT(*) as count FROM users')
                stats['total_users'] = cursor.fetchone()['count']
                
                # Активные пользователи (последние 24 часа)
                cutoff_time = datetime.now(self.kiev_tz) - timedelta(hours=24)
                cursor.execute('''
                    SELECT COUNT(*) as count FROM users 
                    WHERE last_active > ?
                ''', (cutoff_time.isoformat(),))
                stats['active_users_24h'] = cursor.fetchone()['count']
                
                # Общее количество записей фандинга
                cursor.execute('SELECT COUNT(*) as count FROM funding_history')
                stats['total_funding_records'] = cursor.fetchone()['count']
                
                # Записи за последние 24 часа
                cursor.execute('''
                    SELECT COUNT(*) as count FROM funding_history 
                    WHERE timestamp > ?
                ''', (cutoff_time.isoformat(),))
                stats['funding_records_24h'] = cursor.fetchone()['count']
                
                # Количество алертов
                cursor.execute('SELECT COUNT(*) as count FROM alerts')
                stats['total_alerts'] = cursor.fetchone()['count']
                
                # Средний APR за последние 24 часа
                cursor.execute('''
                    SELECT AVG(apr) as avg_apr FROM funding_history 
                    WHERE timestamp > ?
                ''', (cutoff_time.isoformat(),))
                result = cursor.fetchone()
                stats['avg_apr_24h'] = round(result['avg_apr'] or 0, 2)
                
                # Максимальный APR за 24 часа
                cursor.execute('''
                    SELECT MAX(apr) as max_apr FROM funding_history 
                    WHERE timestamp > ?
                ''', (cutoff_time.isoformat(),))
                result = cursor.fetchone()
                stats['max_apr_24h'] = round(result['max_apr'] or 0, 2)
                
                return stats
                
        except Exception as e:
            logger.error(f"Ошибка получения системной статистики: {e}")
            return {}

# Глобальный экземпляр менеджера БД
db_manager = DatabaseManager()
