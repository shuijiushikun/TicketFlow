import asyncio

from create_superuser import create_superuser

if __name__ == "__main__":
    asyncio.run(create_superuser("user@example.com", "very_secure_password"))
