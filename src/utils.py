import logging
from colorama import Fore, Style, init

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("migration.log"),
    ]
)
logger = logging.getLogger(__name__)

def log_info(message):
    """Log info message with cyan color"""
    logger.info(message)
    print(f"{Fore.CYAN}[INFO] {message}{Style.RESET_ALL}")

def log_success(message):
    """Log success message with green color"""
    logger.info(f"SUCCESS: {message}")
    print(f"{Fore.GREEN}[SUCCESS] {message}{Style.RESET_ALL}")

def log_warning(message):
    """Log warning message with yellow color"""
    logger.warning(message)
    print(f"{Fore.YELLOW}[WARNING] {message}{Style.RESET_ALL}")

def log_error(message):
    """Log error message with red color"""
    logger.error(message)
    print(f"{Fore.RED}[ERROR] {message}{Style.RESET_ALL}")

def log_stage(message):
    """Log stage indicator with blue background"""
    logger.info(f"STAGE: {message}")
    print(f"{Fore.WHITE}{Style.BRIGHT}{Fore.BLUE}[STAGE] {message}{Style.RESET_ALL}")

def format_time(seconds):
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def format_number(num):
    """Format number with thousands separators"""
    return f"{num:,}"
