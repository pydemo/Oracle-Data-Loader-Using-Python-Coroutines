# Oracle-Data-Loader-Using-Python-Coroutines
Data loading script for Oracle using Python 3 and coroutines
Make sure you load into different Oracle partitions

```python
time /home/py36/Python-3.6.0b4/python coro_data_load.py  -c chunked_load_test.py
```
##Result
184mil records loaded in 45 min.
(It would take 4.5 hours to load using single SQLLoader session)
```
....
0 max chunk #77 209715200 buffer: 17574400-> 227289600
4 max chunk #77 209715200 buffer: 18006400-> 227721600
1 max chunk #77 209715200 buffer: 18438400-> 228153600
3 max chunk #77 209715200 buffer: 20054400-> 229769600
tail 52534410
after /Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl2.ctl
tail 52534202
tail 52534165
[b'',
 b'SQL*Loader: Release 11.2.0.3.0 - Production on Mon Dec 5 15:08:36 2016',
 b'',
 b'Copyright (c) 1982, 2011, Oracle and/or its affiliates.  All rights reserved'
 b'.',
 b'',
 b'',
 b'Load completed - logical record count 36767879.']
tail 52534286
after /Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl0.ctl
after /Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl4.ctl
tail 52534610
after /Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl1.ctl
[b'',
 b'SQL*Loader: Release 11.2.0.3.0 - Production on Mon Dec 5 15:08:36 2016',
 b'',
 b'Copyright (c) 1982, 2011, Oracle and/or its affiliates.  All rights reserved'
 b'.',
 b'',
 b'',
 b'Load completed - logical record count 36837137.']
[b'',
 b'SQL*Loader: Release 11.2.0.3.0 - Production on Mon Dec 5 15:08:36 2016',
 b'',
 b'Copyright (c) 1982, 2011, Oracle and/or its affiliates.  All rights reserved'
 b'.',
 b'',
 b'',
 b'Load completed - logical record count 36824967.']
after /Bic/scripts/oats/fix/missing_oats/pycopy/kcgm_missing_nw_orders/ctl/all_ctl3.ctl
[b'',
 b'SQL*Loader: Release 11.2.0.3.0 - Production on Mon Dec 5 15:08:36 2016',
 b'',
 b'Copyright (c) 1982, 2011, Oracle and/or its affiliates.  All rights reserved'
 b'.',
 b'',
 b'',
 b'Load completed - logical record count 36763407.']
[b'',
 b'SQL*Loader: Release 11.2.0.3.0 - Production on Mon Dec 5 15:08:36 2016',
 b'',
 b'Copyright (c) 1982, 2011, Oracle and/or its affiliates.  All rights reserved'
 b'.',
 b'',
 b'',
 b'Load completed - logical record count 36772557.']

real    45m52.786s
user    121m50.605s
sys     13m31.441s
```
