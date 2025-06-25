#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для улучшенного фандинг-бота
"""

import pytz
from datetime import datetime, timedelta
from typing import List, Dict
import re

class TimeUtils:
    """Утилиты для работы со временем"""
    
    @staticmethod
    def format_time(timestamp: str, timezone: pytz.timezone) -> str:
        """Форматирование времени в локальный часовой пояс"""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            local_dt = dt.astimezone(timezone)
            return local_dt.strftime('%H:%M:%S')
        except:
            return "N/A"
    
    @staticmethod
    def format_date(timestamp: datetime, timezone: pytz.timezone) -> str:
        """Форматирование даты в локальный часовой пояс"""
        try:
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            local_dt = timestamp.astimezone(timezone)
            return local_dt.strftime('%d.%m.%Y')
        except:
            return "N/A"
    
    @staticmethod
    def time_ago(timestamp: datetime, timezone: pytz.timezone) -> str:
        """Определение времени, прошедшего с момента"""
        try:
            now = datetime.now(timezone)
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            # Приведение к одному часовому поясу
            if timestamp.tzinfo is None:
                timestamp = timezone.localize(timestamp)
            else:
                timestamp = timestamp.astimezone(timezone)
            
            diff = now - timestamp
            
            if diff.seconds < 60:
                return "только что"
            elif diff.seconds < 3600:
                minutes = diff.seconds // 60
                return f"{minutes} мин назад"
            elif diff.days == 0:
                hours = diff.seconds // 3600
                return f"{hours} ч назад"
            else:
                return f"{diff.days} дн назад"
        except:
            return "N/A"

class MessageFormatter:
    """Форматирование сообщений для Telegram"""
    
    @staticmethod
    def format_enhanced_funding_opportunities(opportunities: List, user_deposit: float, 
                                           timezone: pytz.timezone) -> str:
        """Улучшенное форматирование списка возможностей"""
        if not opportunities:
            return "❌ Высокодоходных возможностей не найдено"
        
        current_time = datetime.now(timezone).strftime('%H:%M')
        
        message = f"🔥 **ВЫСОКОДОХОДНЫЕ ВОЗМОЖНОСТИ** 🔥\n"
        message += f"🕒 Время (Киев): **{current_time}**\n\n"
        
        for i, opp in enumerate(opportunities[:10], 1):
            side_emoji = "📈" if opp.side == 'LONG' else "📉"
            
            # Расчет прибыли
            profit_data = ProfitCalculator.calculate_profit(opp.funding_rate, user_deposit)
            
            # Определение уровня возможности
            if opp.apr >= 500:
                level_emoji = "🔥🔥🔥"
                level_text = "ЭКСТРИМ"
            elif opp.apr >= 200:
                level_emoji = "🔥🔥"
                level_text = "ВЫСОКИЙ"
            elif opp.apr >= 100:
                level_emoji = "🔥"
                level_text = "ХОРОШИЙ"
            else:
                level_emoji = "⭐"
                level_text = "СРЕДНИЙ"
            
            message += f"{i}. {side_emoji} **{opp.symbol}** {level_emoji}\n"
            message += f"   💰 APR: **{opp.apr}%** ({level_text})\n"
            message += f"   ⏰ Фандинг: **{opp.funding_rate}%** каждые 8ч\n"
            message += f"   🏛️ Биржа: {opp.exchange}\n"
            
            if profit_data:
                message += f"   💵 Доход/день: **${profit_data['daily_profit']}**\n"
                message += f"   📊 Доход/месяц: **${profit_data['monthly_profit']}**\n"
            
            # Временная метка
            time_info = TimeUtils.time_ago(opp.timestamp, timezone)
            message += f"   🕐 Обновлено: {time_info}\n"
            
            # Инструкции
            if opp.side == 'LONG':
                message += f"   ✅ **ДЕЙСТВИЕ: ЛОНГ** (шорты платят лонгам)\n"
            else:
                message += f"   ✅ **ДЕЙСТВИЕ: ШОРТ** (лонги платят шортам)\n"
            
            message += "\n"
        
        # Итоговая информация
        total_daily_profit = sum(
            ProfitCalculator.calculate_profit(opp.funding_rate, user_deposit).get('daily_profit', 0)
            for opp in opportunities[:5]
        )
        
        message += f"💳 Депозит: **${user_deposit:,.0f}**\n"
        message += f"💰 Потенциал топ-5: **${total_daily_profit:.0f}/день**\n"
        message += f"📊 Найдено возможностей: **{len(opportunities)}**\n"
        message += f"⚡ Включая реал-тайм данные"
        
        return message
    
    @staticmethod
    def format_alert_message(updates: List, timezone: pytz.timezone) -> str:
        """Форматирование алерт-сообщения"""
        if not updates:
            return ""
        
        current_time = datetime.now(timezone).strftime('%H:%M')
        
        message = f"🚨 **АЛЕРТ: ВЫСОКИЙ APR** 🚨\n"
        message += f"🕒 {current_time} (Киев)\n\n"
        
        for update in updates:
            side_emoji = "📈" if update.side == 'LONG' else "📉"
            
            message += f"{side_emoji} **{update.symbol}** ({update.exchange})\n"
            message += f"💰 APR: **{update.apr}%**\n"
            message += f"⏰ Фандинг: **{update.funding_rate}%**\n"
            
            if update.side == 'LONG':
                message += f"✅ **ЛОНГОВАТЬ** (шорты платят)\n"
            else:
                message += f"✅ **ШОРТИТЬ** (лонги платят)\n"
            
            message += "\n"
        
        message += f"⚡ Данные в реальном времени\n"
        message += f"🔍 Детали: /scan или нажмите на кнопку ниже"
        
        return message

class ProfitCalculator:
    """Улучшенный калькулятор прибыли"""
    
    @staticmethod
    def calculate_profit(funding_rate: float, deposit: float, cycles: int = 3) -> Dict:
        """Расчет прибыли с улучшенными метриками"""
        try:
            if deposit <= 0 or abs(funding_rate) > 50:
                raise ValueError("Некорректные входные данные")
            
            # Комиссии (спот + фьючерс)
            spot_commission = 0.001  # 0.1%
            futures_commission = 0.0003  # 0.03%
            total_commission_rate = spot_commission + futures_commission
            
            # Базовые расчеты
            gross_profit_per_cycle = deposit * (abs(funding_rate) / 100)
            commission_per_cycle = deposit * total_commission_rate
            net_profit_per_cycle = gross_profit_per_cycle - commission_per_cycle
            
            # Дневные расчеты
            daily_gross_profit = gross_profit_per_cycle * cycles
            daily_commission = commission_per_cycle * cycles
            daily_net_profit = net_profit_per_cycle * cycles
            
            # Месячные расчеты (90 циклов)
            monthly_cycles = 90
            monthly_gross_profit = gross_profit_per_cycle * monthly_cycles
            monthly_commission = commission_per_cycle * monthly_cycles
            monthly_net_profit = net_profit_per_cycle * monthly_cycles
            
            # Годовые расчеты
            yearly_cycles = cycles * 365
            apr = abs(funding_rate) * yearly_cycles
            
            # ROI метрики
            daily_roi = (daily_net_profit / deposit) * 100
            monthly_roi = (monthly_net_profit / deposit) * 100
            
            # Риск-метрики
            max_drawdown_estimate = deposit * 0.02  # 2% максимальная просадка
            risk_adjusted_return = monthly_net_profit - max_drawdown_estimate
            
            # Время окупаемости
            if daily_net_profit > 0:
                payback_days = deposit / daily_net_profit
            else:
                payback_days = float('inf')
            
            return {
                # Базовые метрики
                'profit_per_cycle': round(net_profit_per_cycle, 2),
                'gross_profit_per_cycle': round(gross_profit_per_cycle, 2),
                'commission_per_cycle': round(commission_per_cycle, 2),
                
                # Дневные метрики
                'daily_profit': round(daily_net_profit, 2),
                'daily_gross_profit': round(daily_gross_profit, 2),
                'daily_commission': round(daily_commission, 2),
                'daily_roi': round(daily_roi, 2),
                
                # Месячные метрики
                'monthly_profit': round(monthly_net_profit, 2),
                'monthly_gross_profit': round(monthly_gross_profit, 2),
                'monthly_commission': round(monthly_commission, 2),
                'monthly_roi': round(monthly_roi, 2),
                
                # Годовые метрики
                'apr': round(apr, 2),
                
                # Риск-метрики
                'max_drawdown_estimate': round(max_drawdown_estimate, 2),
                'risk_adjusted_return': round(risk_adjusted_return, 2),
                'payback_days': round(payback_days, 1) if payback_days != float('inf') else 999,
                
                # Эффективность
                'profit_margin': round((net_profit_per_cycle / gross_profit_per_cycle) * 100, 1),
                'cycles_per_month': monthly_cycles,
                'commission_percentage': round((commission_per_cycle / gross_profit_per_cycle) * 100, 1)
            }
        
        except Exception as e:
            return {}
    
    @staticmethod
    def calculate_compound_profit(funding_rate: float, initial_deposit: float, 
                                days: int, reinvest_percentage: float = 1.0) -> Dict:
        """Расчет сложной прибыли с реинвестированием"""
        try:
            daily_rate = abs(funding_rate) * 3 / 100  # 3 цикла в день
            commission_rate = 0.0013  # 0.13% комиссии за цикл
            net_daily_rate = daily_rate - (commission_rate * 3)
            
            # Расчет с реинвестированием
            current_balance = initial_deposit
            daily_profits = []
            
            for day in range(days):
                daily_profit = current_balance * net_daily_rate
                reinvest_amount = daily_profit * reinvest_percentage
                
                current_balance += reinvest_amount
                daily_profits.append(daily_profit)
            
            total_profit = current_balance - initial_deposit
            
            return {
                'final_balance': round(current_balance, 2),
                'total_profit': round(total_profit, 2),
                'total_return_percentage': round((total_profit / initial_deposit) * 100, 2),
                'average_daily_profit': round(sum(daily_profits) / len(daily_profits), 2),
                'compound_factor': round(current_balance / initial_deposit, 2)
            }
        
        except Exception as e:
            return {}

class ValidationUtils:
    """Утилиты для валидации данных"""
    
    @staticmethod
    def validate_deposit(deposit: str) -> tuple[bool, float, str]:
        """Валидация депозита пользователя"""
        try:
            # Очистка строки от лишних символов
            clean_deposit = re.sub(r'[^\d.]', '', deposit)
            
            if not clean_deposit:
                return False, 0, "Введите числовое значение"
            
            amount = float(clean_deposit)
            
            if amount < 100:
                return False, 0, "Минимальный депозит: $100"
            elif amount > 10_000_000:
                return False, 0, "Максимальный депозит: $10,000,000"
            else:
                return True, amount, "OK"
                
        except ValueError:
            return False, 0, "Некорректное числовое значение"
    
    @staticmethod
    def validate_funding_rate(rate: str) -> tuple[bool, float, str]:
        """Валидация фандинг-рейта"""
        try:
            # Очистка и конвертация
            clean_rate = re.sub(r'[^\d.+-]', '', rate)
            
            if not clean_rate:
                return False, 0, "Введите числовое значение"
            
            funding_rate = float(clean_rate)
            
            if abs(funding_rate) > 10:
                return False, 0, "Фандинг-рейт не может быть больше ±10%"
            else:
                return True, funding_rate, "OK"
                
        except ValueError:
            return False, 0, "Некорректное числовое значение"
    
    @staticmethod
    def validate_symbol(symbol: str) -> tuple[bool, str, str]:
        """Валидация символа криптовалюты"""
        try:
            # Очистка и приведение к верхнему регистру
            clean_symbol = re.sub(r'[^A-Za-z]', '', symbol).upper()
            
            if len(clean_symbol) < 2:
                return False, "", "Символ должен содержать минимум 2 буквы"
            elif len(clean_symbol) > 10:
                return False, "", "Символ не может быть длиннее 10 букв"
            else:
                return True, clean_symbol, "OK"
                
        except Exception:
            return False, "", "Некорректный символ"

class FormatUtils:
    """Утилиты для форматирования"""
    
    @staticmethod
    def format_number(number: float, decimals: int = 2) -> str:
        """Форматирование числа с разделителями тысяч"""
        try:
            return f"{number:,.{decimals}f}"
        except:
            return str(number)
    
    @staticmethod
    def format_percentage(percentage: float, decimals: int = 2) -> str:
        """Форматирование процентов"""
        try:
            return f"{percentage:.{decimals}f}%"
        except:
            return f"{percentage}%"
    
    @staticmethod
    def format_currency(amount: float, currency: str = "USD") -> str:
        """Форматирование валюты"""
        try:
            if currency == "USD":
                return f"${amount:,.2f}"
            else:
                return f"{amount:,.2f} {currency}"
        except:
            return f"{amount} {currency}"
    
    @staticmethod
    def truncate_text(text: str, max_length: int = 100) -> str:
        """Обрезка текста с многоточием"""
        if len(text) <= max_length:
            return text
        else:
            return text[:max_length-3] + "..."
