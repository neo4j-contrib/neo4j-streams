MATCH (n) 
with 
   max(n.realtime) as lastWrite, 
   min(n.realtime) as firstWrite,
   toFloat(count(n)) as total
WITH duration.between(firstWrite, lastWrite).milliseconds as msElapsed,
  total

WITH msElapsed, total, (msElapsed/total) as msPerNode
WITH msElapsed, total, msPerNode, 1000/msPerNode as nodesPerSec

return msElapsed, total, msPerNode, nodesPerSec