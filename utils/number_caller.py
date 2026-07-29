# utils/number_caller.py - Server-controlled number calling system

import asyncio
import logging
import random
from datetime import datetime
from typing import Dict, List, Optional, Set
import json
from database.db import Database

logger = logging.getLogger(__name__)

class NumberCaller:
    """Server-controlled number calling system - Dedicated instance per game tier"""
    
    def __init__(self, tier_name: str = "default"):
        """
        Initialize number caller for a specific tier.
        
        Args:
            tier_name: Name of the tier (e.g., "10birr", "20birr") for logging
        """
        self.tier_name = tier_name
        self.active_games = {}
        self.calling_tasks = {}
        self.countdown_tasks = {}
        self._active_games = {}  # Track which games are actively calling numbers
        self.called_numbers = {}  # Track called numbers per game
        self._manager = None  # Will be set by the game manager
        logger.info(f"NumberCaller initialized for {tier_name}")
    
    def set_game_manager(self, manager):
        """Set the game manager instance for this number caller"""
        self._manager = manager
        logger.info(f"Game manager set for {self.tier_name} number caller")
    
    def is_calling_numbers_for_game(self, game_id: str) -> bool:
        """Check if number calling is active for a specific game"""
        return game_id in self._active_games and self._active_games[game_id]
    
    async def start_number_calling_for_game(self, game_id: str):
        """Start number calling for a game"""
        try:
            # Check if game exists
            game = await Database.get_game(game_id)
            if not game:
                logger.error(f"Game {game_id} not found for {self.tier_name}")
                return False
            
            # Reset called numbers for this game (if restarting)
            self.called_numbers[game_id] = await Database.get_drawn_numbers(game_id)
            logger.info(f"Loaded {len(self.called_numbers[game_id])} existing numbers for game {game_id} ({self.tier_name})")
            
            # Check if already calling
            if game_id in self.calling_tasks and not self.calling_tasks[game_id].done():
                logger.info(f"Already calling numbers for game {game_id} ({self.tier_name})")
                self._active_games[game_id] = True
                return True
            
            # Stop any existing task
            if game_id in self.calling_tasks:
                self.calling_tasks[game_id].cancel()
                try:
                    await self.calling_tasks[game_id]
                except:
                    pass
            
            # Start number calling task
            task = asyncio.create_task(self._call_numbers_for_game(game_id))
            self.calling_tasks[game_id] = task
            
            # Update tracking
            self._active_games[game_id] = True
            
            logger.info(f"Started number calling for game {game_id} ({self.tier_name})")
            return True
            
        except Exception as e:
            logger.error(f"Error starting number calling for {self.tier_name}: {e}")
            return False
    
    async def stop_number_calling_for_game(self, game_id: str):
        """Stop number calling for a game"""
        try:
            # Update tracking
            if game_id in self._active_games:
                self._active_games[game_id] = False
            
            # Cancel calling task
            if game_id in self.calling_tasks:
                task = self.calling_tasks.pop(game_id)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # Cancel countdown task
            if game_id in self.countdown_tasks:
                task = self.countdown_tasks.pop(game_id)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            logger.info(f"Stopped number calling for game {game_id} ({self.tier_name})")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping number calling for {self.tier_name}: {e}")
            return False
    
    async def _call_numbers_for_game(self, game_id: str):
        """Call numbers for a game"""
        try:
            from database.db import Database
            from web_server import websocket_server
            
            logger.info(f"Starting number calling loop for game {game_id} ({self.tier_name})")
            
            # Wait 3 seconds before calling the first number
            await asyncio.sleep(3)
            
            # Initialize called numbers set for this game
            if game_id not in self.called_numbers:
                self.called_numbers[game_id] = await Database.get_drawn_numbers(game_id)
            
            called_stack = self.called_numbers[game_id]
            
            while True:
                try:
                    # Check if game is still active
                    game = await Database.get_game(game_id)
                    if not game:
                        logger.info(f"Game {game_id} not found, stopping number calling ({self.tier_name})")
                        break
                    
                    # Get current status and phase
                    status = game.get('status', '').lower()
                    phase = game.get('current_phase', '').lower()
                    
                    # Game is considered active for number calling if:
                    is_active = (status in ['active', 'game_play'] and 
                                phase not in ['winner_display', 'completed'])
                    
                    if not is_active:
                        logger.info(f"Game {game_id} is not active for number calling (status: {status}, phase: {phase}) ({self.tier_name})")
                        if game_id in self._active_games:
                            self._active_games[game_id] = False
                        break
                    
                    # Check if all numbers have been called
                    if len(called_stack) >= 75:
                        logger.info(f"All 75 numbers have been called for game {game_id} ({self.tier_name})")
                        
                        # Check if there's a winner using the manager
                        winners_count = 0
                        if self._manager and hasattr(self._manager, 'get_winners_count'):
                            winners_count = await self._manager.get_winners_count(game_id)
                        
                        if winners_count == 0:
                            logger.warning(f"Game {game_id} has no winner after all numbers called. Forcing completion... ({self.tier_name})")
                            
                            await Database.update_game_status(game_id, 'completed')
                            await Database.update_game_phase(game_id, 'completed')
                            
                            await websocket_server.broadcast_with_retry({
                                'type': 'game_completed_no_winner',
                                'game_id': game_id,
                                'message': 'Game ended - no winner',
                                'card_price': game.get('card_price', 10),
                                'timestamp': datetime.now().isoformat()
                            })
                            
                            if self._manager and hasattr(self._manager, '_schedule_next_round_after_winner_display'):
                                asyncio.create_task(self._manager._schedule_next_round_after_winner_display(game_id))
                        
                        if game_id in self._active_games:
                            self._active_games[game_id] = False
                        break
                    
                    # Generate new number (1-75)
                    all_numbers = list(range(1, 76))
                    available_numbers = [n for n in all_numbers if n not in called_stack]
                    
                    if not available_numbers:
                        logger.info(f"No available numbers for game {game_id} ({self.tier_name})")
                        if game_id in self._active_games:
                            self._active_games[game_id] = False
                        break
                    
                    # Randomly select next number
                    next_number = random.choice(available_numbers)
                    
                    # Record drawn number
                    success = await Database.record_drawn_number(game_id, next_number)
                    
                    if not success:
                        logger.error(f"Failed to record drawn number {next_number}")
                        await asyncio.sleep(4)
                        continue
                    
                    # Add to called set
                    called_stack.append(next_number)
                    self.called_numbers[game_id] = called_stack
                    
                    # Get bingo letter
                    bingo_letter = self._get_bingo_letter(next_number)
                    
                    # Broadcast new number
                    await websocket_server.broadcast_with_retry({
                        'type': 'number_called',
                        'game_id': game_id,
                        'number': next_number,
                        'letter': bingo_letter,
                        'called_numbers': called_stack,
                        'card_price': game.get('card_price', 10),
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Mark number on all cards using the manager
                    fake_winners = 0
                    if self._manager and hasattr(self._manager, 'mark_number_on_all_cards'):
                        fake_winners = await self._manager.mark_number_on_all_cards(game_id, next_number)
                    
                    # Check if game should be stopped (first winner)
                    game = await Database.get_game(game_id)
                    if game and game.get('status') == 'winner_display':
                        logger.info(f"Game {game_id} entered winner display phase, stopping number calling ({self.tier_name})")
                        break
                    
                    logger.info(f"Called number {next_number} ({bingo_letter}) for game {game_id} ({self.tier_name}) (fake winners: {fake_winners})")
                    
                    # Wait before next number (4 seconds)
                    await asyncio.sleep(4)
                    
                except asyncio.CancelledError:
                    logger.info(f"Number calling cancelled for game {game_id} ({self.tier_name})")
                    if game_id in self._active_games:
                        self._active_games[game_id] = False
                    break
                except Exception as e:
                    logger.error(f"Error in number calling loop ({self.tier_name}): {e}")
                    await asyncio.sleep(4.5)
            
            logger.info(f"Number calling loop ended for game {game_id} ({self.tier_name})")
            
        except Exception as e:
            logger.error(f"Error in _call_numbers_for_game ({self.tier_name}): {e}")
            if game_id in self._active_games:
                self._active_games[game_id] = False
    
    def _get_bingo_letter(self, number: int) -> str:
        """Get BINGO letter for a number"""
        if 1 <= number <= 15:
            return 'B'
        elif 16 <= number <= 30:
            return 'I'
        elif 31 <= number <= 45:
            return 'N'
        elif 46 <= number <= 60:
            return 'G'
        else:  # 61-75
            return 'O'
    
    def get_active_calling_games(self) -> List[str]:
        """Get list of game IDs that are currently calling numbers"""
        active_games = []
        for game_id, is_active in self._active_games.items():
            if is_active:
                active_games.append(game_id)
        return active_games
    
    async def ensure_calling_for_game(self, game_id: str) -> bool:
        """Ensure number calling is active for a game, restart if not"""
        try:
            from database.db import Database
            
            game = await Database.get_game(game_id)
            if not game:
                return False
            
            status = game.get('status', '').lower()
            phase = game.get('current_phase', '').lower()
            
            if status in ['active', 'game_play'] and phase not in ['winner_display', 'completed']:
                if not self.is_calling_numbers_for_game(game_id):
                    logger.warning(f"Number calling not active for game {game_id} ({self.tier_name}), restarting...")
                    return await self.start_number_calling_for_game(game_id)
                return True
            return False
        except Exception as e:
            logger.error(f"Error ensuring number calling for game {game_id} ({self.tier_name}): {e}")
            return False
    
    async def reset_called_numbers_for_game(self, game_id: str):
        """Reset called numbers for a game (when game restarts)"""
        if game_id in self.called_numbers:
            self.called_numbers[game_id] = []
            logger.info(f"Reset called numbers for game {game_id} ({self.tier_name})")
    
    async def cleanup(self):
        """Cleanup all tasks"""
        try:
            for game_id in list(self._active_games.keys()):
                self._active_games[game_id] = False
            
            for game_id, task in list(self.calling_tasks.items()):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            for game_id, task in list(self.countdown_tasks.items()):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            self.calling_tasks.clear()
            self.countdown_tasks.clear()
            self._active_games.clear()
            self.called_numbers.clear()
            
            logger.info(f"NumberCaller cleanup completed for {self.tier_name}")
            
        except Exception as e:
            logger.error(f"Error cleaning up NumberCaller ({self.tier_name}): {e}")

# ==================== GLOBAL INSTANCES ====================
# Each tier gets its own independent number caller
number_caller_10birr = NumberCaller(tier_name="10birr")
number_caller_20birr = NumberCaller(tier_name="20birr")

# For backward compatibility - point to the 10 birr caller
number_caller = number_caller_10birr