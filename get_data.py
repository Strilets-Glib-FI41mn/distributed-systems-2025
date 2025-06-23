
import hazelcast


import argparse
# Defining main function
def main():
    
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("--ip", required=True, help="IP address of the Hazelcast member")
    parser.add_argument("--cluster_name", required=True, help="Name of the Hazelcast cluster")
    parser.add_argument("--map_name", required=True, help="Name of the map to retrieve data from")
    parser.add_argument("-d", "--debug", help="Show debug data", action="store_true")
    args = parser.parse_args()

    try:
        # Initialize Hazelcast client
        client = hazelcast.HazelcastClient(
            cluster_name=args.cluster_name,
            cluster_members=[str(args.ip)]
        )

        if args.debug:
            print(f"Connecting to cluster '{args.cluster_name}' at {args.ip}...")
        
        # Get the map
        my_map = client.get_map(args.map_name).blocking()

        
        print("Keys in the map:")
        for key in my_map.key_set():
            print(f"Key: {key}")


        
        print("Values in the map:")
        for val in my_map.values()():
            print(f"Key: {val}")


        # Print entries in the map
        print("Entries in the map:")
        for key, value in my_map.entry_set():
            print(f"Key: {key}, Value: {value}")

        # Optionally, store results in a list
        res = [(key, value) for key, value in my_map.entry_set()]
        print("All entries:", res)

    except hazelcast.errors.HazelcastSerializationError as e:
        print(f"Serialization error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Shutdown the client
        client.shutdown()
    return
if __name__=="__main__":
    main()