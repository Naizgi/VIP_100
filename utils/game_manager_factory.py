"""
Game Manager Factory - Multi-tier game manager factory
Provides access to game managers for different price tiers (10 birr and 20 birr)
"""
import logging
from typing import Optional, Dict, Any

from utils.game_manager import game_manager, GameManager
from utils.game_manager_20birr import game_manager_20birr, GameManager20Birr

logger = logging.getLogger(__name__)

class GameManagerFactory:
    """
    Factory for managing multiple game manager instances per price tier.
    Supports 10 birr (default) and 20 birr tiers.
    """
    
    def __init__(self):
        # Store managers in a dictionary
        self._managers = {
            10: game_manager,      # 10 birr tier (default)
            20: game_manager_20birr  # 20 birr tier
        }
        self._initialized = {}
        logger.info("✅ GameManagerFactory initialized with tiers: 10, 20")
    
    @property
    def managers(self):
        """Return the managers dictionary for backward compatibility"""
        return self._managers
    
    def get_manager(self, price: Optional[int] = None, game_id: Optional[str] = None):
        """
        Get the appropriate game manager based on price.
        
        Args:
            price: Card price (10 or 20). Defaults to 10 if not specified.
            game_id: Optional game ID - if provided, price will be determined from database.
            
        Returns:
            Game manager instance (GameManager or GameManager20Birr)
        """
        # If game_id is provided, try to determine price from database
        if game_id is not None and price is None:
            try:
                from database.db import Database
                with Database.get_cursor() as cursor:
                    cursor.execute("SELECT card_price FROM games WHERE game_id = ?", (game_id,))
                    result = cursor.fetchone()
                    if result:
                        price = result[0] or 10
                        logger.debug(f"Detected price {price} for game {game_id}")
                    else:
                        price = 10
            except Exception as e:
                logger.warning(f"Could not determine price for game {game_id}: {e}")
                price = 10
        
        # Default to 10 if price is None or not supported
        if price is None or price not in self._managers:
            price = 10
        
        manager = self._managers[price]
        logger.debug(f"Returning manager for price {price}: {manager.__class__.__name__}")
        return manager
    
    def get_active_game(self, price: Optional[int] = None) -> Optional[Dict]:
        """
        Get the active game for a specific price tier.
        
        Args:
            price: Card price (10 or 20). Defaults to 10.
            
        Returns:
            Active game dict or None
        """
        manager = self.get_manager(price)
        return manager.active_game
    
    async def get_all_active_games(self) -> Dict[int, Optional[Dict]]:
        """
        Get all active games across all price tiers.
        
        Returns:
            Dict mapping price to active game dict (or None)
        """
        result = {}
        for price, manager in self._managers.items():
            try:
                # Try async method first
                if hasattr(manager, 'get_active_round_game'):
                    game = await manager.get_active_round_game()
                    result[price] = game
                else:
                    # Fallback to sync property
                    result[price] = manager.active_game
            except Exception as e:
                logger.warning(f"Error getting active game for price {price}: {e}")
                result[price] = None
        return result
    
    async def initialize_manager(self, price: int = 10) -> bool:
        """
        Initialize a specific manager.
        
        Args:
            price: Card price (10 or 20)
            
        Returns:
            True if initialized successfully
        """
        if price in self._initialized and self._initialized[price]:
            logger.debug(f"Manager for price {price} already initialized")
            return True
        
        manager = self.get_manager(price)
        
        # Check if manager has initialize method
        if hasattr(manager, 'initialize'):
            try:
                # Check if it's async
                import inspect
                if inspect.iscoroutinefunction(manager.initialize):
                    result = await manager.initialize()
                else:
                    result = manager.initialize()
                self._initialized[price] = True
                logger.info(f"✅ Initialized manager for {price} birr tier")
                return result if result is not None else True
            except Exception as e:
                logger.error(f"❌ Failed to initialize manager for {price} birr tier: {e}")
                return False
        
        # Manager doesn't need initialization
        self._initialized[price] = True
        return True
    
    async def initialize_all(self) -> bool:
        """Initialize all game managers."""
        success = True
        for price in self._managers.keys():
            if not await self.initialize_manager(price):
                success = False
                logger.warning(f"⚠️ Failed to initialize manager for {price} birr tier")
        return success
    
    async def cleanup(self):
        """Clean up all game managers."""
        for price, manager in self._managers.items():
            if hasattr(manager, 'cleanup'):
                try:
                    # Check if it's async
                    import inspect
                    if inspect.iscoroutinefunction(manager.cleanup):
                        await manager.cleanup()
                    else:
                        manager.cleanup()
                    logger.info(f"🧹 Cleaned up manager for {price} birr tier")
                except Exception as e:
                    logger.error(f"❌ Failed to cleanup manager for {price} birr tier: {e}")
        
        self._initialized.clear()
        logger.info("✅ All game managers cleaned up")
    
    def get_manager_by_price(self, price: int):
        """Get manager by price (shortcut)."""
        return self.get_manager(price)
    
    @property
    def default_manager(self):
        """Get the default (10 birr) manager."""
        return self._managers[10]
    
    @property
    def manager_20birr(self):
        """Get the 20 birr manager."""
        return self._managers[20]

# ==================== SINGLETON INSTANCE ====================
game_manager_factory = GameManagerFactory()

# For backward compatibility - expose commonly used methods directly
get_manager = game_manager_factory.get_manager
get_active_game = game_manager_factory.get_active_game

__all__ = [
    'game_manager_factory',
    'GameManagerFactory',
    'get_manager',
    'get_active_game'
]