import os
import redis
import sys
#from dotenv import load_dotenv


def main():

   #load_dotenv()
    redis_host = os.getenv("REDIS_HOST")
    redis_port = int(os.getenv("REDIS_PORT"))
    redis_password = os.getenv("REDIS_PASSWORD")

    #print (redis_host,redis_port,redis_password)
    try:
        # Connecting to Redis
        r = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            password=redis_password, 
            decode_responses=True
        )
        
        # Checking Connection
        if r.ping():
            print(f"Connected to Redis at {redis_host}:{redis_port}")
        
        print("Job starting to add 10000 accounts...")
        pipe = r.pipeline()
        for i in r.keys('ACC-*'):
            pipe.delete(i)
        
        pipe.execute()
        
        for i in range(1, 10001):
            acc_id = f"ACC-{i:06d}" 
            name = f"Customer_{i}"
            email = f"customer_{i}@banking.local"
            
            pipe.set(f"{acc_id}:name", name)
            pipe.set(f"{acc_id}:email", email)

        pipe.execute()
        
        count = len(r.keys('ACC-*'))
        print(f"Completed! Found {count} keys into the Database.")

    except Exception as e:
        print(f"Error at connection state or seeding: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()