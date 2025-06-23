
import hazelcast


import argparse
# Defining main function
def main():
    
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("--ip")
    parser.add_argument("--cluster_name")
    parser.add_argument("--map_name")
    args = parser.parse_args()


    if (args.ip is not None) and (args.cluster_name is not None) and (args.map_name is not None):
        client = hazelcast.HazelcastClient(
        cluster_name=args.cluster_name,
        cluster_members=[
            str(args.ip)
        ]
        
        )
        my_map = client.get_map(args.map_name).blocking()
        print([(key, value) for key, value in my_map.entry_set()])
    return
if __name__=="__main__":
    main()