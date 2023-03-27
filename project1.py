from mrjob.job import MRJob
from mrjob.step import MRStep


# ---------------------------------!!! Attention Please!!!------------------------------------
# Please add more details to the comments for each function. Clarifying the input 
# and the output format would be better. It's helpful for tutors to review your code.

# Using multiple MRSteps is permitted, please name your functions properly for readability.

# We will test your code with the following comand:
# "python3 project1.py -r hadoop /Users/benedictachun/Downloads/tiny-data.txt  -o hdfs_output --jobconf mapreduce.job.reduces=2"

# Please make sure that your code can be compiled before submission.
# ---------------------------------!!! Attention Please!!!------------------------------------

class proj1(MRJob):    

    def mapper(self, _, line):
        # Split input into user ID, location ID and check-in time
        user_id, loc_id, check_in_time = line.split(',', 3)
        if len(user_id) and len(loc_id):
            pair = user_id+","+loc_id
            if len(check_in_time):
                yield pair, 1
                # Emit special token in form: (user_id, *) along with value 1
                yield user_id+",*", 1

    def combiner(self, key, values):
        yield key, sum(values)
    
    def reducer_init(self):
        self.marginal = 1 
    
    # First reducer that will be run through in the 1st step
    def reducer(self, key, values):
        # Split input into user ID and location ID
        user_id, loc_id = key.split(",", 1)
        # If input is special token sum value to track total count of user_id
        if loc_id == "*":
            self.marginal = sum(values)
        else:
            count = sum(values)
            # If input not a special token, compute check-in probability
            checkin_prob = count/self.marginal
            emit_key = loc_id+","+str(checkin_prob)+","+user_id
            # Emit key for sorting in second partitioner(JOBCONF1) before running 2nd step
            yield emit_key, None
    
    # Second reducer that will be run through in the 2nd step
    def reducer2(self, key, _):
        # Split intput into location ID, check-in probability and user ID
        loc_id, checkin_prob, user_id = key.split(",", 3) 
        yield loc_id, f"{user_id},{checkin_prob}"
            
    SORT_VALUES = True

    def steps(self):
        # First configuration
        JOBCONF = { 
        'mapreduce.map.output.key.field.separator':',',
        # Partition map outputs by the 1st field (User ID)
        'mapreduce.partition.keypartitioner.options':'-k1,1'
        }
        # Second configuration
        JOBCONF1 = { 
        'mapreduce.map.output.key.field.separator':',',
        # Partition output from the first step by the 1st field (Location ID)
        'mapreduce.partition.keypartitioner.options':'-k1,1',
        'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        # Sort by first field(location ID), second field(check-in probability) by numerical sorting in descending order then by third field(User ID)
        'mapreduce.partition.keycomparator.options': '-k1,1 -k2,2nr -k3,3'
        }

        # The first step runs through mapper, combiner, job configuration and first reducer to compute check-in probability (not sorted)
        # In the second step the output of the first step is passed into the second job configuration for sorting and then passed into the final reducer
        return [
            MRStep(jobconf=JOBCONF, mapper=self.mapper, combiner=self.combiner, reducer_init=self.reducer_init, reducer=self.reducer),
            MRStep(jobconf=JOBCONF1, reducer=self.reducer2)
        ]

if __name__ == '__main__':
    proj1.run()

