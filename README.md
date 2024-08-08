# Lab 2 Report


## Usage

Clone a cluster of group-42, select the jar file from our S3 bucket at `S3://Lab2-something.jar`. Run a spark-submit application with the following parameters in order:
- sealevel: integer (can be positive or negative, decimals and string are strictly not allowed)
- enter <name> where name is the name of orc file that should be at `S3://abs-tudelft-sbd-2022/<name>.orc`
The next 4 parameters are latitudes and longitudes for parquet file pushdown filters
- lower limit of latitude
- higher limit of latitude
- lower limit of longitude
- higher limit of longitude
- The name of output orc files.

An example for running netherlands dataset is below. The last 7 inputs should be provided as parameters to spark-submit.
```bash
spark-submit --deploy-mode cluster s3://sbd-group-42/Lab2-assembly-1.0.jar 12 netherlands 50 54 3 8 output
```

## Approach


### Iteration 0: Baseline

We will consider for our baseline, the final implementation of Lab 1.

| | |
|---|---|
| System                  | HP ZBook G5 |
| Workers                 | 12 | 
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:05:24 | 
| RAM | 16 GB |
| Sealevel | 12 |
 
To clarify, we saw some mixed use of terminology. Workers, vCPUs and threads are assumed to mean the same thing and we will use workers from here on. Cores however are different from threads because they can be hyperthreaded i.e. 1 core can run 2 (or more) threads.
 
### Iteration 1: Running the same task on AWS Cluster

For the first iteration we have re-used the code from assignment 1 so the same optimizations are already in here (we have solved all objectives of assignment 1 including excellent). They can be found [here](https://github.com/abs-tudelft-sbd-2022/lab-1-2022-group-42/#scalability) (this links to our README of assignment 1 to which you should have access). The only relevant change is the use of push down filters when reading the parquet files. For the first assigment, we wanted to read the totality of the parquet files, but in this case, the parquet files include the whole world, while we just need a certain part of them for each dataset. To optimize this, we used push down filters when reading the parquet files. Since the parquet files include row-group statistics, it allows us to discard directly row-groups we don't need, based on latitude and longitude ranges defined.
 
Since we don't yet know how this will play out, to get a feel of AWS we did some experiments. In hindsight, we used way too many workers (20) and got misled by the first experiment. We realised later that we should have started with lesser hardware, about a quarter of what we thought, was a decent enough number.

| | |
|---|---|
| System                  | AWS Cluster |
| Workers                 | 20 |
| EC2 instance            | 5 x m5.xlarge |
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:56:00 | 
| RAM | 90 GB |
| Sealevel | 12 |

This ran longer than expected. After taking a look at the Spark history server, we noticed writing the output files took specially long: 17 minutes (compared to a few seconds on a local machine and we wrote a whole paragraph in lab 1 complaining that a few seconds is too long). Also the show associated to computation of the dataset `dataSetFinalJoin` and `dataSetExcellent` took 11 and 24 minutes, respectively.  
 
Since we are able to observe better results on a local machine, we suspect the selected hardware can be better.

<p align="center">
<img src="https://i.imgur.com/tX2w6Fb.png">
</p>
<h6 align="center "> Figure 1: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/gIR8riQ.png">
</p>
<h6 align="center "> Figure 2: Memory Usage  </h6>

These graphs were obtained with the Ganglia tool. Looking at the CPU load, we can see that at certain points, the CPU usage reaches around 90%. At the same time, when looking at cluster memory, we always had, at least, 20GB available.  
Due to this, we decided to change our hardware to nodes with more CPU power. With our eyes in the future runs, we ended up maintaining memory. We wanted to select nodes with more processing power and lesser memory but could not provision the nodes we wanted because of demand. After changing the hardware, we obtained the following results:

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c4.2xlarge |
| Workers                 | 40 | 
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:41:00 | 
| RAM | 90 GB |
| Sealevel | 12 |


<p align="center">
<img src="https://i.imgur.com/ld5s1yL.png">
</p>
<h6 align="center "> Figure 3: CPU Usage  </h6>


<p align="center">g
<img src="https://i.imgur.com/uu0RPP7.png">
</p>
<h6 align="center "> Figure 4: Memory Usage  </h6>

As we can see, we had less stress regarding the CPU usage, due to the increased processing power of the hardware chosen. When it comes to memory, it stayed the same since these nodes have the same memory. This gives us confidence that we can use the same hardware for the next (bigger) dataset.


### Iteration 2: Bigger Dataset

In this iteration we tried using our application on a bigger dataset to see how it would behave, and the next bigger dataset was France. We chose an increase of sealevel of 300 since this is around the average elevation of France and we should see interesting results because the joins should now be between datasets with sizable number of rows in each.

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c4.2xlarge |
| Workers                 | 40 | 
| Dataset                 | France | 
| Run time <br>(hh:mm:ss) | 01:15:00 | 
| RAM | 90 GB |
| Sealevel | 300 |

CPU Usage
<p align="center">
<img src="https://i.imgur.com/YXZWaxO.png">
</p>
<h6 align="center "> Figure 5: CPU Usage  </h6>

Memory Usage
<p align="center">
<img src="https://i.imgur.com/8Ou1YZJ.png">
</p>
<h6 align="center "> Figure 6: Memory Usage  </h6>

CPU usage is still below 80%, which leads us to believe we wouldn't gain much for allocating more resources. When it comes to memory, we can see an increase here, which was expected, since a bigger input dataset will lead with bigger dataset variables and therefore more memory is needed to cache them. Nevertheless, we still have a headroom of about 10% regarding memory.

The first orc file (according to assignment) took 21 minutes to write 530KB (which is a really long time for a small file). The second write took 0.7 seconds to write 2.4KB which seems appropriate but then that is also too long for 2.4KB. In the same run, reading orc + parquet files + calculating H3index and doing a join took 7 minutes. Writing small orc files is taking about a third of total compute time, omething is clearly wrong here. It is not a problem of IOPS because the EC2 instances (also checked for m4.large) and S3 should perform much better according to their documentations. We suspect a load balancer or subnet is rate limiting us when writing. We can not do anything about this, we considered removing all writes because we don't quite care about the output, in this assignment we are more interested in time taken.

We noticed something else, the computation of the following code took 31 minutes to run. Looking at this piece of code, we could not understand why it took so long to compute, since `union` shouldn't cause any shuffling according to documentation and the code is very simple basic Spark (which is supposed to be highly optimised).
 
<p align="center">
<img src="https://i.imgur.com/jFOGNDm.png">
</p>
<h6 align="center "> Figure 7:  </h6>

<p align="center">
<img src="https://i.imgur.com/NgOMw9X.png">
</p>
<h6 align="center "> Figure 8:  </h6>

In order to try and solve this, after looking at the code again, we realized we performed a `union` operation just to join the dataset containing evacuees to cities with the one containing the evacuees to the Harbour to only write them in the orc files, therefore, we took out this union and simply did two separate writes, where the second one appends to first one.
 
On the HP machine we use, this improved run time by a few seconds so we tested this on AWS on netherlands and saw a small improvement. Since the Netherlands dataset is small, the union was not really expensive to start with. France, however showed improvement.
 
<p align="center">
<img src="https://i.imgur.com/7EWmo90.png">
</p>
<h6 align="center "> Figure 9:  </h6>

<p align="center">
<img src="https://i.imgur.com/eu09mHq.png">
</p>
<h6 align="center "> Figure 10:  </h6>

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c5.2xlarge |
| Workers                 | 40 | 
| Dataset                 | France | 
| Run time <br>(hh:mm:ss) | 01:04:00 | 
| RAM | 90 GB |
| Sealevel | 300 |

### Iteration 3: Simplifying

We felt this run time is still way too high for our application, tried a random something. We took out all the `show` and `unpersist` commands and just let Spark optimise for us. This resulted in a huge performance increase.

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c5.2xlarge |
| Workers                 | 40 | 
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:05:00 | 
| RAM | 90 GB |
| Sealevel | 12 |

These were the results we were expecting coming in to this lab and slightly better than our laptop. Let's take a look at our resource usage:

<p align="center">
<img src="https://i.imgur.com/G3Ah7j5.png">
</p>
<h6 align="center "> Figure 11: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/1ZC1cgh.png">
</p>
<h6 align="center "> Figure 12: Memory Usage  </h6>

As we can see, our resources clearly exceed the required for this job, so we scaled down our hardware:

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 2 x m3.xlarge |
| Workers                 | 8 | 
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:13:00 | 
| RAM | 36 GB |
| Sealevel | 12 |

We switched back to m type instances because we were have difficulties allocating c type instances due to spot availability.

<p align="center">
<img src="https://i.imgur.com/WyLqdSq.png">
</p>
<h6 align="center "> Figure 12: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/QEym9nm.png">
</p>
<h6 align="center "> Figure 13: Memory Usage  </h6>

For the rest of our iterations, we will keep the same configuration and number of core instances, until we face bottlenecks.

### Iteration 4: Trying to optimize memory usage

Now we will try to add back the `unpersist` after a cached dataset is no longer needed to better manage memory. First we tried it on our local machine:

| | |
|---|---|
| System                  | HP ZBook G5 |
| Workers                 | 12 | 
| Dataset                 | Netherlands | 
| Run time <br>(hh:mm:ss) | 00:15:00 + | 
| RAM | 16 GB |
| Sealevel | 12 |

It ran for atleast 15 minutes before we cancelled this run, since we were looking at, at least, a 3x increase in run time, even if it came at a potential memory usage decrease.

To check if it had made any improvement in memory usage, we also ran it on AWS so we could take a look at memory usage:


<p align="center">
<img src="https://i.imgur.com/z0oyejP.png">
</p>
<h6 align="center "> Figure 16: Memory Usage  </h6>

As we can see, it didn't result in a memory usage improvement, and therefore we reverted these changes.


### Iteration 5: France Dataset

Now we feel confident in our application, we will run it with a bigger dataset, France.

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c5.2xlarge |
| Workers                 | 40 | 
| Dataset                 | France | 
| Run time <br>(hh:mm:ss) | 00:08:20 | 
| RAM | 90 GB |
| Sealevel | 300 |

<p align="center">
<img src="https://i.imgur.com/ZZzMLsL.png">
</p>
<h6 align="center "> Figure 14: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/jciMWmP.png">
</p>
<h6 align="center "> Figure 15: Memory Usage  </h6>

This run time was expected.


### Iteration 6: North America

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c5.2xlarge |
| Workers                 | 40 | 
| Dataset                 | North America | 
| Run time <br>(hh:mm:ss) | 01:11:00 | 
| RAM | 90 GB |
| Sealevel | 300 |

<p align="center">
<img src="https://i.imgur.com/iPAMRnw.png">
</p>
<h6 align="center "> Figure 17: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/PbRUuwv.png">
</p>
<h6 align="center "> Figure 18: Memory Usage  </h6>

Looking at the Server load distribution, we can clearly see that the workload is unevenly distributed:

<p align="center">
<img src="https://i.imgur.com/e371GWv.png">
</p>
<h6 align="center "> Figure 19: Server Load Distribution  </h6>

Digging a bit deeper in the Spark history server, we can see that the input data for each task is not balanced, since the max input is a lot bigger than the median, as it can be seen:

<p align="center">
<img src="https://i.imgur.com/Iugwz33.png">
</p>
<h6 align="center "> Figure 20: Task Inputs </h6>

In order to optimize this, we tried repartitioning the dataset contanting the filtered Orc dataset `dataSetCities` before joining it with the dataset containing the h3indexes and elevation `dataSetElevation`, however we could not reach the sweet spot for the number of partitions where our application performed better.

### Iteration 7: Europe

Despite not being able to fix the poor partioning problems identified in the previous iteration, we still ran our application with the next biggest dataset, Europe, to see how our application would perform.

| | |
|---|---|
| System                  | AWS Cluster |
| EC2 instance            | 5 x c5.2xlarge |
| Workers                 | 40 | 
| Dataset                 | Europe | 
| Run time <br>(hh:mm:ss) | 01:35:00 | 
| RAM | 90 GB |
| Sealevel | 100 |

For this run, we used Sealevel 100 because after a quick research, we learned that more than half of Europe is below 180m, we used a value a little below that.

<p align="center">
<img src="https://i.imgur.com/eFtO0K8.png">
</p>
<h6 align="center "> Figure 21: CPU Usage  </h6>


<p align="center">
<img src="https://i.imgur.com/lTMqThk.png">
</p>
<h6 align="center "> Figure 22: Memory Usage  </h6>

### Iteration 8: World

For this iteration, we would run it with the dataset containing the whole world. Due to some time constraints and difficulties due to spot instances availability, we weren't able to run.  
Taking a look at how our application scaled through the datasets, we would predict it would take around 3-4 hours to run our application with the World dataset.




## Summary of application-level improvements

Optimizations carried over from Lab1 (caching, selecting only relevant columns, filter ordering ...) .
Push down filters for optimizing Parquet files loading.
Removed unecessary operations, like `union` and `show` that were slowing down the code.

 
## Cluster-level improvements

Through the use of the Ganglia tool, we could identify any possible bottlenecks and adjust the hardware respectively.  
For the Netherlands dataset, we scaled down our hardware to lower our resource usage altough it came at a cost, since run time increased.  
For the rest of the iterations we kept the same hardware as long as we didn't face any bottlenecks, although we could have adjusted the hardware for each dataset for optimal use.




## Conclusion

In this Lab, we got to understand Spark a little deeper in order to optmize our application, through a more extended use of Spark history server, for example.  
We also got to use AWS to spawn a cluster to run our application, and used tools like Ganglia to adjust our cluster hardware.  


