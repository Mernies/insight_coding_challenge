import json
import time
import datetime
import statistics

# load data
data = []
users = {}
maxtime = 0
mintime = 0
outfile = open('venmo_output/output.txt','w')

with open('venmo_input/venmo-trans.txt') as infile:
    count = 0
    for line in infile:

        # Extract JSON information
        linedata = json.loads(line)
        actor = linedata['actor']
        target = linedata['target']
        timestamp = linedata['created_time']

        # Convert timestamp into seconds
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
        curr_time = time.mktime(dt_obj.timetuple())

        # Update max time if necessary
        if curr_time > maxtime:
            maxtime = curr_time
            mintime = maxtime - 60

            # Remove expired entries
            if data:
                for entry in data:
                    user1 = entry[0]
                    user2 = entry[1]
                    entrytime = int(entry[2])
                    if entrytime < mintime:
                        try:
                            users[user1].remove(user2)
                            users[user2].remove(user1)
                        except ValueError:
                            count = count

        # Skip incomplete entries and expired entries (>60 seconds from max time)
        if curr_time < mintime or not actor or not target:
            continue

        # Connect users (actor and target)
        if not users.get(actor):
            users[actor] = []
        users[actor].append(target)
        users[actor] = list(set(users[actor]))

        if not users.get(target):
            users[target] = []
        users[target].append(actor)
        users[target] = list(set(users[target]))

        count = count + 1

        # Store data
        data.append([actor,target,curr_time])

        # Calculate median
        degrees = []
        for entry,connections in users.items():
            temp_degree = len(connections)

            # Exclude 0 degree nodes (deleted nodes)
            if temp_degree > 0:
                degrees.append(len(connections))

        median = statistics.median(degrees)

        # Output median
        median_string = '{:3.2f}'.format(median) + '\n'
        outfile.write(median_string)
