#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Улучшенный Telegram бот для мониторинга фандинг-рейтов v3.0
С реал-тайм мониторингом и поддержкой киевского времени
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz
import json
import schedule

import telebot
from telebot import types
from concurrent.futures import ThreadPoolExecutor

# Импорт наших модулей
from config import bot_config, websocket_config, parsing_config, TOP_50_COINS
from database import db_manager, UserProfile
from real_time_monitor import create_real_time_monitor, FundingUpdate
from data_collector import EnhancedDataCollector
from utils import MessageFormatter, ProfitCalculator, TimeUtils

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('funding_bot_v3.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedFundingBot:
    """Улучшенный фандинг-бот с реал-тайм мониторингом"""
    
    def __init__(self, token: str):
        self.bot = telebot.TeleBot(token, parse_mode='Markdown')
        self.db = db_manager
        self.data_collector = EnhancedDataCollector()
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.kiev_tz = pytz.timezone('Europe/Kiev')
        
        # Реал-тайм мониторинг
        self.rt_monitor = None
        self.rt_monitor_task = None
        self.monitoring_active = False
        
        # Планировщик задач
        self.scheduler_thread = None
        self.scheduler_active = False
        
        # Кэш последних возможностей
        self.last_opportunities = []
        self.last_update_time = None
        
        self.setup_handlers()
        self.setup_scheduler()
        logger.info("🤖 Улучшенный фандинг-бот v3.0 инициализирован")
    
    def setup_handlers(self):
        """Настройка обработчиков команд"""
        
        @self.bot.message_handler(commands=['start'])
        def start_command(message):
            try:
                user_id = message.from_user.id
                user_name = message.from_user.first_name or ""
                
                # Создание/обновление пользователя
                self.db.create_or_update_user(user_id)
                self.db.update_user_activity(user_id)
                
                self.send_main_menu(message.chat.id, user_name)
                
                logger.info(f"👤 Новый пользователь: {user_id} ({user_name})")
                
            except Exception as e:
                logger.error(f"Ошибка команды start: {e}")
                self.bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте позже.")
        
        @self.bot.message_handler(commands=['stats'])
        def stats_command(message):
            try:
                self.handle_user_statistics(message.chat.id)
            except Exception as e:
                logger.error(f"Ошибка команды stats: {e}")
        
        @self.bot.message_handler(commands=['alerts'])
        def alerts_command(message):
            try:
                self.handle_user_alerts(message.chat.id)
            except Exception as e:
                logger.error(f"Ошибка команды alerts: {e}")
        
        @self.bot.message_handler(commands=['monitor'])
        def monitor_command(message):
            try:
                if message.from_user.id == bot_config.admin_chat_id:
                    self.handle_monitor_control(message.chat.id)
                else:
                    self.bot.send_message(message.chat.id, "❌ Недостаточно прав")
            except Exception as e:
                logger.error(f"Ошибка команды monitor: {e}")
        
        @self.bot.callback_query_handler(func=lambda call: True)
        def callback_handler(call):
            try:
                user_id = call.from_user.id
                self.db.update_user_activity(user_id)
                self.handle_callback(call)
            except Exception as e:
                logger.error(f"Ошибка callback: {e}")
                self.bot.answer_callback_query(call.id, "❌ Произошла ошибка")
        
        @self.bot.message_handler(content_types=['text'])
        def text_handler(message):
            try:
                user_id = message.from_user.id
                self.db.update_user_activity(user_id)
                self.handle_text_message(message)
            except Exception as e:
                logger.error(f"Ошибка обработки текста: {e}")
                self.bot.send_message(message.chat.id, "❌ Произошла ошибка обработки сообщения")
        
        @self.bot.message_handler(content_types=['location'])
        def location_handler(message):
            try:
                self.handle_location(message)
            except Exception as e:
                logger.error(f"Ошибка обработки геолокации: {e}")
    
    def send_main_menu(self, chat_id: int, user_name: str = ""):
        """Отправка главного меню с информацией о реал-тайм мониторинге"""
        try:
            # Получение текущего времени в Киеве
            kiev_time = datetime.now(self.kiev_tz).strftime('%H:%M')
            
            text = f"🔥 **ФАНДИНГ СКАНЕР v3.0** 🔥\n\n"
            text += f"Привет, {user_name}! 👋\n\n" if user_name else "Добро пожаловать! 👋\n\n"
            
            text += f"🕒 Ваше время (Киев): **{kiev_time}**\n\n"
            
            text += "🎯 **Новые возможности v3.0:**\n"
            text += "⚡ Реал-тайм мониторинг через WebSocket\n"
            text += "🔔 Автоматические алерты при APR >100%\n"
            text += "📊 Множественные источники данных\n"
            text += "🇺🇦 Поддержка киевского времени\n"
            text += "📈 Расширенная аналитика и статистика\n\n"
            
            # Статус мониторинга
            monitor_status = "🟢 Активен" if self.monitoring_active else "🔴 Неактивен"
            text += f"📡 Реал-тайм мониторинг: {monitor_status}\n\n"
            
            # Последние возможности
            if self.last_opportunities:
                best_apr = max(opp.apr for opp in self.last_opportunities[:3])
                text += f"🔥 Лучший текущий APR: **{best_apr}%**\n\n"
            
            text += "⚡ Выберите действие:"
            
            markup = types.InlineKeyboardMarkup(row_width=2)
            
            # Основные кнопки
            scan_btn = types.InlineKeyboardButton("🔍 Сканировать", callback_data="scan_funding")
            realtime_btn = types.InlineKeyboardButton("⚡ Реал-тайм", callback_data="realtime_data")
            
            calc_btn = types.InlineKeyboardButton("💰 Калькулятор", callback_data="profit_calculator")
            alerts_btn = types.InlineKeyboardButton("🔔 Алерты", callback_data="user_alerts")
            
            stats_btn = types.InlineKeyboardButton("📊 Статистика", callback_data="user_stats")
            settings_btn = types.InlineKeyboardButton("⚙️ Настройки", callback_data="settings")
            
            help_btn = types.InlineKeyboardButton("❓ Справка", callback_data="help")
            
            markup.add(scan_btn, realtime_btn)
            markup.add(calc_btn, alerts_btn)
            markup.add(stats_btn, settings_btn)
            markup.add(help_btn)
            
            self.bot.send_message(chat_id, text, reply_markup=markup)
            
        except Exception as e:
            logger.error(f"Ошибка отправки главного меню: {e}")
    
    def handle_callback(self, call):
        """Обработка callback запросов"""
        try:
            chat_id = call.message.chat.id
            data = call.data
            
            # Роутинг команд
            if data == "scan_funding":
                self.handle_scan_funding(chat_id)
            elif data == "realtime_data":
                self.handle_realtime_data(chat_id)
            elif data == "profit_calculator":
                self.handle_profit_calculator(chat_id)
            elif data == "user_alerts":
                self.handle_user_alerts(chat_id)
            elif data == "user_stats":
                self.handle_user_statistics(chat_id)
            elif data == "settings":
                self.handle_settings(chat_id)
            elif data == "help":
                self.handle_help(chat_id)
            elif data == "back_to_menu":
                user_name = call.from_user.first_name or ""
                self.send_main_menu(chat_id, user_name)
            elif data.startswith("set_deposit_"):
                amount = int(data.split("_")[2])
                self.set_user_deposit(chat_id, amount)
            elif data.startswith("quick_calc_"):
                parts = data.split("_")
                deposit = float(parts[2])
                funding_rate = float(parts[3])
                self.send_quick_calculation(chat_id, deposit, funding_rate)
            elif data.startswith("alert_"):
                alert_id = int(data.split("_")[1])
                self.mark_alert_read(chat_id, alert_id)
            else:
                logger.warning(f"Неизвестный callback: {data}")
            
            self.bot.answer_callback_query(call.id)
            
        except Exception as e:
            logger.error(f"Ошибка обработки callback: {e}")
            self.bot.answer_callback_query(call.id, "❌ Ошибка")
    
    def handle_realtime_data(self, chat_id: int):
        """Обработка запроса реал-тайм данных"""
        try:
            if not self.monitoring_active:
                text = "📡 **РЕАЛ-ТАЙМ МОНИТОРИНГ** 📡\n\n"
                text += "⚠️ Реал-тайм мониторинг в данный момент неактивен\n\n"
                text += "🔧 Мониторинг запускается автоматически при старте бота\n"
                text += "📞 Обратитесь к администратору для активации"
                
                markup = types.InlineKeyboardMarkup()
                menu_btn = types.InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu")
                markup.add(menu_btn)
                
                self.bot.send_message(chat_id, text, reply_markup=markup)
                return
            
            # Получение статистики мониторинга
            monitor_stats = self.rt_monitor.get_stats() if self.rt_monitor else {}
            
            text = f"📡 **РЕАЛ-ТАЙМ МОНИТОРИНГ** 📡\n\n"
            text += f"🟢 **Статус**: Активен\n"
            text += f"📊 **Активные подключения**: {monitor_stats.get('active_connections', 0)}\n"
            text += f"📈 **Обновлений получено**: {monitor_stats.get('total_updates', 0)}\n"
            text += f"🔔 **Алертов отправлено**: {monitor_stats.get('alerts_sent', 0)}\n"
            text += f"💾 **Отслеживается символов**: {monitor_stats.get('tracked_symbols', 0)}\n\n"
            
            # Последние возможности из реал-тайм данных
            if self.last_opportunities:
                text += f"🔥 **ПОСЛЕДНИЕ ВОЗМОЖНОСТИ:**\n\n"
                
                for i, opp in enumerate(self.last_opportunities[:5], 1):
                    side_emoji = "📈" if opp.side == 'LONG' else "📉"
                    time_ago = TimeUtils.time_ago(opp.timestamp, self.kiev_tz)
                    
                    text += f"{i}. {side_emoji} **{opp.symbol}** ({opp.exchange})\n"
                    text += f"   💰 APR: **{opp.apr}%**\n"
                    text += f"   ⏰ Обновлено: {time_ago}\n\n"
            else:
                text += "📭 Пока нет реал-тайм данных\n\n"
            
            text += f"🕒 Последнее обновление: {datetime.now(self.kiev_tz).strftime('%H:%M:%S')}"
            
            markup = types.InlineKeyboardMarkup()
            refresh_btn = types.InlineKeyboardButton("🔄 Обновить", callback_data="realtime_data")
            alerts_btn = types.InlineKeyboardButton("🔔 Мои алерты", callback_data="user_alerts")
            menu_btn = types.InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu")
            
            markup.add(refresh_btn, alerts_btn)
            markup.add(menu_btn)
            
            self.bot.send_message(chat_id, text, reply_markup=markup)
            
        except Exception as e:
            logger.error(f"Ошибка обработки реал-тайм данных: {e}")
            self.bot.send_message(chat_id, "❌ Ошибка получения реал-тайм данных")
    
    def handle_scan_funding(self, chat_id: int):
        """Обработка запроса сканирования с множественными источниками"""
        try:
            # Получение профиля пользователя
            profile = self.db.get_user_profile(chat_id)
            user_deposit = profile.deposit if profile else 1000
            
            # Отправка сообщения о загрузке
            loading_msg = self.bot.send_message(
                chat_id,
                "🔍 **Сканирую множественные источники...**\n\n"
                "⏳ CoinGlass, Binance, Bybit, OKX, Gate.io\n"
                "🔄 Фильтрую альткоины вне топ-50\n"
                "📊 Рассчитываю прибыль для вашего депозита\n"
                "💡 Применяю реал-тайм корректировки"
            )
            
            # Асинхронное сканирование в отдельном потоке
            future = self.executor.submit(
                self.enhanced_scan_wrapper, 
                chat_id, 
                loading_msg.message_id, 
                user_deposit
            )
            
        except Exception as e:
            logger.error(f"Ошибка запуска расширенного сканирования: {e}")
            self.bot.send_message(chat_id, "❌ Ошибка запуска сканирования")
    
    def enhanced_scan_wrapper(self, chat_id: int, loading_msg_id: int, user_deposit: float):
        """Обертка для расширенного сканирования"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Сбор данных из множественных источников
                opportunities = loop.run_until_complete(
                    self.data_collector.collect_enhanced_funding_data()
                )
                
                # Объединение с реал-тайм данными
                if self.last_opportunities:
                    # Фильтрация дубликатов и объединение
                    rt_symbols = {opp.symbol for opp in self.last_opportunities}
                    
                    for rt_opp in self.last_opportunities:
                        # Замена данных реал-тайм данными если они свежее
                        found = False
                        for i, opp in enumerate(opportunities):
                            if opp.symbol == rt_opp.symbol and opp.exchange == rt_opp.exchange:
                                if rt_opp.timestamp > opp.timestamp:
                                    opportunities[i] = rt_opp
                                found = True
                                break
                        
                        if not found and rt_opp.apr >= bot_config.min_apr_threshold:
                            opportunities.append(rt_opp)
                
                # Сортировка по APR
                opportunities.sort(key=lambda x: x.apr, reverse=True)
                opportunities = opportunities[:bot_config.max_opportunities_display]
                
                # Сохранение в БД
                if opportunities:
                    self.db.save_funding_updates(opportunities)
                
                # Обновление статистики пользователя
                self.db.update_user_stats(chat_id, total_scans=1)
                
                # Форматирование сообщения
                message = MessageFormatter.format_enhanced_funding_opportunities(
                    opportunities, user_deposit, self.kiev_tz
                )
                
                # Создание клавиатуры
                markup = types.InlineKeyboardMarkup()
                
                refresh_btn = types.InlineKeyboardButton("🔄 Обновить", callback_data="scan_funding")
                realtime_btn = types.InlineKeyboardButton("⚡ Реал-тайм", callback_data="realtime_data")
                calc_btn = types.InlineKeyboardButton("💰 Калькулятор", callback_data="profit_calculator")
                
                markup.add(refresh_btn, realtime_btn)
                markup.add(calc_btn)
                
                # Быстрые расчеты для топ-3 возможностей
                if opportunities[:3]:
                    markup.row()
                    for i, opp in enumerate(opportunities[:3]):
                        quick_btn = types.InlineKeyboardButton(
                            f"📊 {opp.symbol}",
                            callback_data=f"quick_calc_{user_deposit}_{opp.funding_rate}"
                        )
                        markup.insert(quick_btn)
                
                menu_btn = types.InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu")
                markup.add(menu_btn)
                
                # Отправка результата
                self.bot.delete_message(chat_id, loading_msg_id)
                self.bot.send_message(chat_id, message, reply_markup=markup)
                
                logger.info(f"✅ Сканирование завершено для пользователя {chat_id}: {len(opportunities)} возможностей")
                
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"Ошибка расширенного сканирования: {e}")
            try:
                self.bot.edit_message_text(
                    "❌ Ошибка при сканировании множественных источников\n\n🔄 Попробуйте позже",
                    chat_id, loading_msg_id
                )
            except:
                pass
    
    def handle_user_alerts(self, chat_id: int):
        """Обработка запроса алертов пользователя"""
        try:
            # Получение алертов пользователя
            alerts = self.db.get_user_alerts(chat_id, limit=10)
            unread_alerts = self.db.get_user_alerts(chat_id, unread_only=True)
            
            text = f"🔔 **МОИ АЛЕРТЫ** 🔔\n\n"
            
            if unread_alerts:
                text += f"🆕 **Непрочитанных**: {len(unread_alerts)}\n\n"
                
                for alert in unread_alerts[:5]:
                    alert_time = TimeUtils.format_time(alert['sent_at'], self.kiev_tz)
                    side_emoji = "📈" if alert['funding_rate'] < 0 else "📉"
                    
                    text += f"{side_emoji} **{alert['symbol']}** ({alert['exchange']})\n"
                    text += f"   💰 APR: **{alert['apr']}%**\n"
                    text += f"   🕒 {alert_time}\n\n"
            
            if alerts:
                text += f"📋 **Всего алертов**: {len(alerts)}\n"
                text += f"📅 **Период**: последние 7 дней\n\n"
            else:
                text += "📭 У вас пока нет алертов\n\n"
                text += "💡 Алерты отправляются автоматически при APR >100%\n"
                text += "⚙️ Настройте порог в разделе Настройки"
            
            markup = types.InlineKeyboardMarkup()
            
            if unread_alerts:
                mark_read_btn = types.InlineKeyboardButton(
                    "✅ Отметить прочитанными", 
                    callback_data="mark_alerts_read"
                )
                markup.add(mark_read_btn)
            
            settings_btn = types.InlineKeyboardButton("⚙️ Настройки алертов", callback_data="alert_settings")
            realtime_btn = types.InlineKeyboardButton("⚡ Реал-тайм", callback_data="realtime_data")
            menu_btn = types.InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu")
            
            markup.add(settings_btn, realtime_btn)
            markup.add(menu_btn)
            
            self.bot.send_message(chat_id, text, reply_markup=markup)
            
        except Exception as e:
            logger.error(f"Ошибка обработки алертов: {e}")
            self.bot.send_message(chat_id, "❌ Ошибка получения алертов")
    
    def handle_user_statistics(self, chat_id: int):
        """Обработка запроса статистики пользователя"""
        try:
            # Получение статистики пользователя
            user_stats = self.db.get_user_stats(chat_id)
            profile = self.db.get_user_profile(chat_id)
            
            text = f"📊 **ВАША СТАТИСТИКА** 📊\n\n"
            
            if profile:
                registration_date = TimeUtils.format_date(profile.created_at, self.kiev_tz)
                text += f"👤 **Профиль**:\n"
                text += f"   📅 Регистрация: {registration_date}\n"
                text += f"   💳 Депозит: ${profile.deposit:,.0f}\n"
                text += f"   📊 Мин. APR: {profile.min_apr}%\n"
                text += f"   🌍 Часовой пояс: {profile.timezone}\n\n"
            
            text += f"📈 **Активность**:\n"
            text += f"   🔍 Сканирований: **{user_stats.get('total_scans', 0)}**\n"
            text += f"   🔔 Алертов получено: **{user_stats.get('alerts_received', 0)}**\n"
            text += f"   🏆 Лучший APR: **{user_stats.get('best_apr_found', 0)}%**\n"
            text += f"   💰 Расчетов прибыли: **${user_stats.get('total_profit_calculated', 0):,.0f}**\n\n"
            
            # Системная статистика
            system_stats = self.db.get_system_stats()
            if system_stats:
                text += f"🌐 **Общая статистика**:\n"
                text += f"   👥 Пользователей: {system_stats.get('total_users', 0)}\n"
                text += f"   📊 Записей фандинга: {system_stats.get('total_funding_records', 0)}\n"
                text += f"   📈 Средний APR (24ч): {system_stats.get('avg_apr_24h', 0)}%\n"
                text += f"   🔥 Максимальный APR (24ч): {system_stats.get('max_apr_24h', 0)}%\n"
            
            markup = types.InlineKeyboardMarkup()
            
            scan_btn = types.InlineKeyboardButton("🔍 Сканировать", callback_data="scan_funding")
            calc_btn = types.InlineKeyboardButton("💰 Калькулятор", callback_data="profit_calculator")
            settings_btn = types.InlineKeyboardButton("⚙️ Настройки", callback_data="settings")
            menu_btn = types.InlineKeyboardButton("🏠 Меню", callback_data="back_to_menu")
            
            markup.add(scan_btn, calc_btn)
            markup.add(settings_btn)
            markup.add(menu_btn)
            
            self.bot.send_message(chat_id, text, reply_markup=markup)
            
        except Exception as e:
            logger.error(f"Ошибка обработки статистики: {e}")
            self.bot.send_message(chat_id, "❌ Ошибка получения статистики")
    
    # Реал-тайм обработка
    async def rt_update_callback(self, updates: List[FundingUpdate]):
        """Callback для обработки реал-тайм обновлений"""
        try:
            # Обновление кэша последних возможностей
            for update in updates:
                # Поиск существующей записи
                found_index = None
                for i, existing in enumerate(self.last_opportunities):
                    if existing.symbol == update.symbol and existing.exchange == update.exchange:
                        found_index = i
                        break
                
                if found_index is not None:
                    self.last_opportunities[found_index] = update
                else:
                    self.last_opportunities.append(update)
            
            # Сортировка и ограничение
            self.last_opportunities.sort(key=lambda x: x.apr, reverse=True)
            self.last_opportunities = self.last_opportunities[:20]
            
            # Сохранение в БД
            self.db.save_funding_updates(updates)
            
            # Отправка алертов пользователям
            await self.send_user_alerts(updates)
            
            logger.info(f"📬 Обработано {len(updates)} реал-тайм обновлений")
            
        except Exception as e:
            logger.error(f"Ошибка обработки реал-тайм обновлений: {e}")
    
    async def send_user_alerts(self, updates: List[FundingUpdate]):
        """Отправка алертов пользователям"""
        try:
            # Получение пользователей с включенными алертами
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT user_id, min_apr, timezone FROM users 
                    WHERE auto_alerts = 1 AND notifications = 1
                ''')
                users = cursor.fetchall()
            
            for user in users:
                user_id = user['user_id']
                min_apr = user['min_apr']
                user_tz = pytz.timezone(user['timezone'])
                
                # Фильтрация обновлений по порогу пользователя
                user_alerts = [upd for upd in updates if upd.apr >= min_apr]
                
                if user_alerts:
                    # Отправка алерта
                    alert_text = MessageFormatter.format_alert_message(
                        user_alerts[:3], user_tz
                    )
                    
                    try:
                        self.bot.send_message(user_id, alert_text, parse_mode='Markdown')
                        
                        # Сохранение алерта в БД
                        for alert in user_alerts[:3]:
                            self.db.save_alert(
                                user_id, alert.symbol, alert.funding_rate,
                                alert.apr, alert.exchange, 'realtime_alert'
                            )
                        
                        logger.info(f"📬 Алерт отправлен пользователю {user_id}")
                        
                    except Exception as e:
                        logger.warning(f"Не удалось отправить алерт пользователю {user_id}: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка отправки алертов пользователям: {e}")
    
    # Планировщик задач
    def setup_scheduler(self):
        """Настройка планировщика задач"""
        try:
            # Очистка старых данных каждые 6 часов
            schedule.every(6).hours.do(self.scheduled_cleanup)
            
            # Системная статистика каждый час
            schedule.every().hour.do(self.update_system_metrics)
            
            # Резервное копирование БД каждые 24 часа
            schedule.every().day.at("03:00").do(self.backup_database)
            
            logger.info("⏰ Планировщик задач настроен")
            
        except Exception as e:
            logger.error(f"Ошибка настройки планировщика: {e}")
    
    def start_scheduler(self):
        """Запуск планировщика в отдельном потоке"""
        try:
            self.scheduler_active = True
            
            def run_scheduler():
                while self.scheduler_active:
                    schedule.run_pending()
                    time.sleep(60)  # Проверка каждую минуту
            
            self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            logger.info("⏰ Планировщик задач запущен")
            
        except Exception as e:
            logger.error(f"Ошибка запуска планировщика: {e}")
    
    def scheduled_cleanup(self):
        """Плановая очистка старых данных"""
        try:
            self.db.cleanup_old_data()
            logger.info("🧹 Выполнена плановая очистка данных")
        except Exception as e:
            logger.error(f"Ошибка плановой очистки: {e}")
    
    def update_system_metrics(self):
        """Обновление системных метрик"""
        try:
            current_time = datetime.now(self.kiev_tz)
            
            # Сохранение метрик
            if self.rt_monitor:
                stats = self.rt_monitor.get_stats()
                self.db.save_system_metric('rt_active_connections', stats.get('active_connections', 0))
                self.db.save_system_metric('rt_total_updates', stats.get('total_updates', 0))
                self.db.save_system_metric('rt_alerts_sent', stats.get('alerts_sent', 0))
            
            logger.info("📊 Системные метрики обновлены")
            
        except Exception as e:
            logger.error(f"Ошибка обновления метрик: {e}")
    
    def backup_database(self):
        """Резервное копирование базы данных"""
        try:
            import shutil
            import os
            
            source_db = self.db.db_path
            backup_name = f"backup_{datetime.now(self.kiev_tz).strftime('%Y%m%d_%H%M%S')}.db"
            backup_path = os.path.join("backups", backup_name)
            
            # Создание папки для бэкапов
            os.makedirs("backups", exist_ok=True)
            
            # Копирование файла БД
            shutil.copy2(source_db, backup_path)
            
            logger.info(f"💾 Создан бэкап БД: {backup_name}")
            
        except Exception as e:
            logger.error(f"Ошибка создания бэкапа: {e}")
    
    # Запуск и остановка
    async def start_realtime_monitoring(self):
        """Запуск реал-тайм мониторинга"""
        try:
            if not self.monitoring_active:
                logger.info("🚀 Запуск реал-тайм мониторинга...")
                
                self.rt_monitor = create_real_time_monitor(self.rt_update_callback)
                self.rt_monitor_task = asyncio.create_task(self.rt_monitor.start_monitoring())
                self.monitoring_active = True
                
                logger.info("✅ Реал-тайм мониторинг запущен")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска реал-тайм мониторинга: {e}")
    
    async def stop_realtime_monitoring(self):
        """Остановка реал-тайм мониторинга"""
        try:
            if self.monitoring_active:
                logger.info("🛑 Остановка реал-тайм мониторинга...")
                
                self.monitoring_active = False
                
                if self.rt_monitor:
                    await self.rt_monitor.stop_monitoring()
                
                if self.rt_monitor_task:
                    self.rt_monitor_task.cancel()
                
                logger.info("✅ Реал-тайм мониторинг остановлен")
        except Exception as e:
            logger.error(f"❌ Ошибка остановки реал-тайм мониторинга: {e}")
    
    def run(self):
        """Запуск бота"""
        try:
            logger.info("🚀 Запуск улучшенного фандинг-бота v3.0")
            
            # Запуск планировщика
            self.start_scheduler()
            
            # Запуск реал-тайм мониторинга в отдельном потоке
            def start_rt_monitoring():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.start_realtime_monitoring())
                loop.run_forever()
            
            rt_thread = threading.Thread(target=start_rt_monitoring, daemon=True)
            rt_thread.start()
            
            logger.info("✅ Все системы готовы")
            
            # Основной цикл бота
            self.bot.infinity_polling(
                timeout=10,
                long_polling_timeout=5,
                logger_level=logging.INFO,
                allowed_updates=['message', 'callback_query', 'location']
            )
            
        except KeyboardInterrupt:
            logger.info("👋 Остановка бота по запросу пользователя")
        except Exception as e:
            logger.error(f"💥 Критическая ошибка бота: {e}")
        finally:
            # Очистка ресурсов
            self.cleanup()
    
    def cleanup(self):
        """Очистка ресурсов при завершении"""
        try:
            logger.info("🧹 Очистка ресурсов...")
            
            # Остановка планировщика
            self.scheduler_active = False
            
            # Остановка executor
            self.executor.shutdown(wait=True)
            
            # Остановка реал-тайм мониторинга
            if self.monitoring_active:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.stop_realtime_monitoring())
                loop.close()
            
            logger.info("✅ Ресурсы очищены")
            
        except Exception as e:
            logger.error(f"Ошибка очистки ресурсов: {e}")

# Точка входа
def main():
    """Главная функция запуска"""
    
    # Проверка токена
    token = bot_config.telegram_token
    if token == "YOUR_TELEGRAM_BOT_TOKEN":
        print("❌ Установите токен бота в переменной окружения TELEGRAM_TOKEN")
        print("💡 Или обновите config.py")
        return 1
    
    try:
        # Создание и запуск бота
        bot = EnhancedFundingBot(token)
        bot.run()
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка запуска: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
