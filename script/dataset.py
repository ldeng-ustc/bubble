#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datasets import *
for ds in DATASETS:
    density = ds.ecount / ds.vcount
    print(f"{ds.name}: {density:.2f}")


# In[ ]:




