# Large Scale Data Processing: Project 3
## Group 2 - Zheng Zhou, Zehua Zhang

## Findings of our group
1. 
| Graph file              | MIS file                     | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

2. 
| Graph file              | Iterations | Running time |
| ----------------------- | ---------- | ------------ |
| small_edges.csv         | 1          | 2s           |
| line_100_edges.csv      | 3          | 3s           |
| twitter_100_edges.csv   | 2          | 3s           |
| twitter_1000_edges.csv  | 2          | 3s           |
| twitter_10000_edges.csv | 4          | 6s           |

All are verified with our verifyMIS function.

3.a.

|                                       | 3 x 4 cores | 4 x 2 cores | 2 x 2 cores |
| ------------------------------------- | ----------- | ----------- | ----------- |
| Running time                          | 903s        | 1192s       | 1326s       |
| Remaining vertices after 1 iteration  | 7884546     | 6837292     | 7382372     |
| Remaining vertices after 2 iterations | 48391       | 35731       | 37819       |
| Remaining vertices after 3 iterations | 381         | 435         | 529         |
| Remaining vertices after 4 iterations | 1           | 2           | 2           |
| Remaining vertices after 5 iterations | 0           | 0           | 0           |
| Number of iterations                  | 5           | 5           | 5           |

All are verified with our verifyMIS function.  

  b. With 3 x 4 cores, Luby's algorithm ran the fastest (903s). With 4 x 2, it ran 1192s. With 2 x 2 cores, it ran 1326s. We found that the more cores generally reduce the running time more significantly than only increasing the nodes.

  




