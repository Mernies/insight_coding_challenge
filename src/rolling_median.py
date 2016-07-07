import json
import time
import datetime
import statistics

# Convert a timestamp into total seconds
def convert_timestamp(timestamp):
    temp = timestamp.split('T')
    date = temp[0]
    mil_time = temp[1]
    mil_time = mil_time[:-1]
    temp = date.split('-')
    year = int(temp[0])
    month = int(temp[1])
    day = int(temp[2])
    temp = mil_time.split(':')
    hour = int(temp[0])
    minute = int(temp[1])
    second = int(temp[2])

    dt_obj = datetime.datetime(year,month,day,hour,minute,second)
    totalseconds = time.mktime(dt_obj.timetuple())
    return totalseconds

# Remove expired data entries
def remove_expired_data(data):
    if data:

        # data is stored as [user1, user2, entrytime (in total seconds)]
        for entry in data:
            user1 = entry[0]
            user2 = entry[1]
            entrytime = int(entry[2])
            if entrytime < mintime:
                try:
                    users[user1].remove(user2)
                    users[user2].remove(user1)
                except ValueError:
                    count = 0
    return data

# Connect users
def connect_users(userlist,user1,user2):

    # Add user node if it doesn't exist then connect to other user
    # Repeat for second user
    if not userlist.get(user1):
        userlist[user1] = []
    userlist[user1].append(user2)
    userlist[user1] = list(set(userlist[user1]))

    if not userlist.get(user2):
        userlist[user2] = []
    userlist[user2].append(user1)
    userlist[user2] = list(set(userlist[user2]))

    return userlist

# Calculate median
def calculate_median(userlist):
    degrees = []
    for entry,connections in userlist.items():
        temp_degree = len(connections)

        # Exclude 0 degree nodes (deleted nodes)
        if temp_degree > 0:
            degrees.append(len(connections))

    median = statistics.median(degrees)
    return median

# Write median to output file
def output_median(median,outfile):
    median_string = '{:3.2f}'.format(median) + '\n'
    outfile.write(median_string)

# load data
data = []
users = {}
maxtime = 0
mintime = 0
median = 0
outfile = open('venmo_output/output.txt','w')

# Loop over each line in input file
with open('venmo_input/venmo-trans.txt') as infile:
    for line in infile:

        # Extract JSON information
        linedata = json.loads(line)
        actor = linedata['actor']
        target = linedata['target']
        timestamp = linedata['created_time']

        # Convert timestamp to total seconds
        curr_time = convert_timestamp(timestamp)

        # Update max time if necessary
        if curr_time > maxtime:
            maxtime = curr_time
            mintime = maxtime - 60

            # Remove expired data
            data = remove_expired_data(data)

        # Skip incomplete entries and expired entries (>60 seconds from max time)
        if curr_time < mintime or not actor or not target:
            output_median(median,outfile)
            continue

        # Connect actor and target to each other
        users = connect_users(users,actor,target)

        # Store data entry
        data.append([actor,target,curr_time])

        # Calculate median
        median = calculate_median(users)

        # Output median
        output_median(median,outfile)
