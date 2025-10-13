from faker import Faker
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models.user_model import User
from app.repositories.user_repository import get_pwd_hash
import random

fake = Faker('pt_BR')

def generate_fake_users(n: int = 50) -> list[User]:
    """Generate n fake users with realistic data"""
    
    roles = ['admin', 'user']
    statuses = ['active', 'inactive']
    notifications = ['yes', 'no']
    
    users = []
    
    for _ in range(n):
        
        user = User(
            id=fake.uuid4(),  
            name=fake.name(),
            email=fake.unique.email(),  
            password=get_pwd_hash('password123'),  
            phone_number=fake.msisdn()[:20],  # Número de telefone brasileiro
            role=random.choice(roles),
            signatureStatus=random.choice(statuses),
            notifications=random.choice(notifications),
        )
        users.append(user)
    
    return users


def seed_users(count: int = 50, clear_existing: bool = False):
    """Seed the database with fake users"""
    
    db: Session = SessionLocal()
    
    try:
        # Check existing users
        existing_count = db.query(User).count()
        
        if existing_count > 0:
            print(f"⚠️  Database already has {existing_count} users.")
            
            if clear_existing:
                print("🗑️  Clearing existing users...")
                db.query(User).delete()
                db.commit()
                print("✅ All users deleted!")
            else:
                print("ℹ️  Use --clear flag to remove existing users first.")
                return
        
        # Generate fake users
        print(f"🔄 Generating {count} fake users...")
        fake_users = generate_fake_users(count)
        
        # Insert users
        print("💾 Inserting users into database...")
        db.bulk_save_objects(fake_users)
        db.commit()
        
        print(f"✅ Successfully seeded {count} users!")
        print("📧 All users have the password: password123")
        
        # Show some examples
        print("\n📋 Sample users:")
        sample_users = db.query(User).limit(5).all()
        for user in sample_users:
            print(f"  - {user.name} ({user.email}) - Role: {user.role}")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error seeding database: {e}")
        raise
    finally:
        db.close()


def clear_users():
    """Clear all users from database"""
    
    db: Session = SessionLocal()
    
    try:
        count = db.query(User).count()
        
        if count == 0:
            print("ℹ️  Database is already empty.")
            return
        
        print(f"🗑️  Deleting {count} users...")
        db.query(User).delete()
        db.commit()
        
        print(f"✅ Successfully deleted {count} users!")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error clearing database: {e}")
        raise
    finally:
        db.close()


def show_stats():
    """Show database statistics"""
    
    db: Session = SessionLocal()
    
    try:
        total = db.query(User).count()
        print("\n📊 Database Statistics:")
        print(f"  Total users: {total}")
        
        if total > 0:
            
            print("\n  By Role:")
            roles = db.query(User.role, db.func.count(User.id)).group_by(User.role).all()
            for role, count in roles:
                print(f"    - {role}: {count}")
            
            
            print("\n  By Signature Status:")
            statuses = db.query(User.signatureStatus, db.func.count(User.id)).group_by(User.signatureStatus).all()
            for status, count in statuses:
                print(f"    - {status}: {count}")
            
            
            print("\n  By Notifications:")
            notifications = db.query(User.notifications, db.func.count(User.id)).group_by(User.notifications).all()
            for notif, count in notifications:
                print(f"    - {notif}: {count}")
        
    except Exception as e:
        print(f"❌ Error getting statistics: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Seed database with fake users')
    parser.add_argument('action', choices=['seed', 'clear', 'stats'], help='Action to perform')
    parser.add_argument('--count', '-c', type=int, default=50, help='Number of users to create (default: 50)')
    parser.add_argument('--clear', action='store_true', help='Clear existing users before seeding')
    
    args = parser.parse_args()
    
    if args.action == 'seed':
        seed_users(count=args.count, clear_existing=args.clear)
    elif args.action == 'clear':
        clear_users()
    elif args.action == 'stats':
        show_stats()