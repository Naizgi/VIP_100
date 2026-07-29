"""
Game Manager Factory - Provides the appropriate game manager based on price
Supports 10 birr (default) and 20 birr tiers
"""
import logging
from typing import Optional

from utils.game_manager import game_manager, GameManager
from utils.game_manager_20birr import game_manager_20birr, GameManager20Birr

logger = logging.getLogger(__name__)

class GameManagerFactory:
    """Factory for getting the appropriate game manager based on price"""
    
    def __init__(self):
        self._managers = {}
        self._default_manager = game_manager
        self._manager_20birr = game_manager_20birr
        
        # Map price to manager instance
        self._managers = {
            10: self._default_manager,
            20: self._manager_20birr
        }
        
        # Track which managers are initialized
        self._initialized = {}
        
        logger.info("GameManagerFactory initialized with 10 birr and 20 birr tiers")
    
    def get_manager(self, price: Optional[int] = None, game_id: Optional[str] = None) -> object:
        """
        Get the appropriate game manager based on price
        
        Args:
            price: Card price (10 or 20). If None, returns default (10 birr)
            game_id: Optional game ID - if provided, price will be determined from database
            
        Returns:
            Game manager instance (GameManager or GameManager20Birr)
        """
        # If game_id is provided, determine price from database
        if game_id and price is None:
            try:
                from database.db import Database
                with Database.get_cursor() as cursor:
                    cursor.execute("SELECT card_price FROM games WHERE game_id = ?", (game_id,))
                    result = cursor.fetchone()
                    if result:
                        price = result['card_price'] or 10
                        logger.info(f"Detected price {price} for game {game_id}")
                    else:
                        price = 10
            except Exception as e:
                logger.warning(f"Could not determine price for game {game_id}, using default 10: {e}")
                price = 10
        
        # Default to 10 if price is None or not in our map
        if price is None or price not in self._managers:
            price = 10
        
        manager = self._managers[price]
        logger.debug(f"Returning manager for price {price}: {manager.__class__.__name__}")
        return manager
    
    def get_active_game(self, price: Optional[int] = None) -> Optional[dict]:
        """
        Get the active game for a specific price
        
        Args:
            price: Card price (10 or 20). If None, returns active game for default (10 birr)
            
        Returns:
            Active game dict or None
        """
        manager = self.get_manager(price)
        return manager.active_game
    
    async def initialize_manager(self, price: int = 10) -> bool:
        """
        Initialize a specific manager
        
        Args:
            price: Card price (10 or 20)
            
        Returns:
            True if initialized successfully
        """
        if price in self._initialized and self._initialized[price]:
            logger.info(f"Manager for price {price} already initialized")
            return True
        
        manager = self.get_manager(price)
        
        # Check if manager has initialize method
        if hasattr(manager, 'initialize'):
            try:
                result = await manager.initialize()
                self._initialized[price] = True
                logger.info(f"Initialized manager for price {price}")
                return result
            except Exception as e:
                logger.error(f"Failed to initialize manager for price {price}: {e}")
                return False
        
        # Manager doesn't need initialization
        self._initialized[price] = True
        return True
    
    async def initialize_all(self) -> bool:
        """Initialize all game managers"""
        success = True
        for price in self._managers.keys():
            if not await self.initialize_manager(price):
                success = False
                logger.warning(f"Failed to initialize manager for price {price}")
        
        return success
    
    async def cleanup(self):
        """Clean up all game managers"""
        for price, manager in self._managers.items():
            if hasattr(manager, 'cleanup'):
                try:
                    await manager.cleanup()
                    logger.info(f"Cleaned up manager for price {price}")
                except Exception as e:
                    logger.error(f"Failed to cleanup manager for price {price}: {e}")
        
        self._initialized.clear()
        logger.info("All game managers cleaned up")
    
    def get_all_active_games(self) -> dict:
        """Get all active games across all prices"""
        result = {}
        for price, manager in self._managers.items():
            if manager.active_game:
                result[price] = manager.active_game
        return result
    
    def get_manager_by_price(self, price: int) -> object:
        """Get manager by price (shortcut)"""
        return self.get_manager(price)
    
    @property
    def default_manager(self):
        """Get the default (10 birr) manager"""
        return self._default_manager
    
    @property
    def manager_20birr(self):
        """Get the 20 birr manager"""
        return self._manager_20birr

# Singleton instance
game_manager_factory = GameManagerFactory()

# For backward compatibility - expose the factory methods directly
get_manager = game_manager_factory.get_manager
get_active_game = game_manager_factory.get_active_game

__all__ = [
    'game_manager_factory',
    'GameManagerFactory',
    'get_manager',
    'get_active_game'
]