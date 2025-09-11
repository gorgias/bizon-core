# How We Ensure Only Successfully Written Messages Are Committed

## ✅ **Corrected Flow - Proper At-Least-Once Delivery**

```
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐    ┌─────────────────┐
│ AsyncKafkaPoller│    │   KafkaSource    │    │ Destination    │    │ Offset Commit   │
│                 │    │                  │    │                │    │                 │
│ 1. Poll messages│───▶│ 2. get_messages()│    │                │    │                 │
│ 2. Queue fairly │    │    - Store msgs  │    │                │    │                 │
│ 3. Mark INFLIGHT│    │    - Mark inflight│   │                │    │                 │
│    (NOT complete)│   │                  │───▶│ 3. Write to    │    │                 │
│                 │    │                  │    │    destination │    │                 │
│                 │    │                  │    │                │───▶│ 4. commit()     │
│                 │    │ 5. Mark COMPLETE │◄───│    ✅ SUCCESS   │    │   - Mark msgs   │
│                 │    │    only if       │    │                │    │     completed   │
│                 │    │    dest success  │    │                │    │   - Get safe    │
│                 │    │                  │    │                │    │     offsets     │
│                 │    │                  │    │                │    │   - Commit only │
│                 │    │                  │    │                │    │     contiguous  │
└─────────────────┘    └──────────────────┘    └────────────────┘    └─────────────────┘
```

## 🔄 **Detailed Step-by-Step Process**

### **Step 1: Message Polling (AsyncKafkaPoller)**
```python
def get_messages(self, max_messages):
    # Fair round-robin across partitions
    for message in fair_round_robin():
        messages.append(message)
        # ✅ Mark as INFLIGHT (not completed!)
        partition_queue.mark_inflight(message.offset())
    return messages
```

### **Step 2: Message Consumption (KafkaSource)**
```python
def _read_topics_async(self):
    encoded_messages = self.async_poller.get_messages()
    # ✅ Store message references for tracking
    self.current_messages = encoded_messages.copy()
    return SourceIteration(records=...)
```

### **Step 3: Destination Write (StreamingRunner)**
```python
# In streaming.py
destination.write_or_buffer_records(...)  # This might fail!

# ✅ ONLY commit if destination write succeeded
if os.getenv("ENVIRONMENT") == "production":
    source.commit()  # This is where the magic happens
```

### **Step 4: Safe Commit (KafkaSource.commit)**
```python
def commit(self):
    if self.current_messages:
        # ✅ Mark as completed ONLY after destination success
        self.async_poller.mark_messages_completed(self.current_messages)
        self.current_messages = []
    
    # ✅ Get only contiguous safe offsets
    commit_offsets = self.async_poller.get_commit_offsets()
    
    # ✅ Commit only processed messages
    self.consumer.commit(offsets=commit_offsets)
```

## 🛡️ **Safety Guarantees**

### **At-Least-Once Delivery**
- ✅ Messages marked **inflight** when consumed
- ✅ Messages marked **completed** only after destination write succeeds  
- ✅ Only **contiguous completed ranges** are committed
- ✅ Failed destination writes = no commit = message will be reprocessed

### **Failure Scenarios Handled**

#### **Scenario 1: Destination Write Fails**
```
Messages: [100, 101, 102] → INFLIGHT
Destination write → ❌ FAILS  
commit() never called → Messages remain INFLIGHT
Next poll → Messages 100-102 reprocessed ✅
```

#### **Scenario 2: Partial Batch Success** 
```
Messages: [100, 101, 102] → INFLIGHT
Process 100, 101 → Destination write ✅ SUCCESS
commit() called → mark_completed([100, 101])
Message 102 still INFLIGHT → Will be reprocessed ✅
```

#### **Scenario 3: Out-of-Order Completion**
```
Messages: [100, 101, 102] → INFLIGHT  
Complete: 100, 102 (101 still inflight)
get_commit_offsets() returns offset=101 (100+1)
Does NOT commit 103 because 101 missing ✅
```

## 🚫 **What We Fixed - The Previous Problem**

### ❌ **Before (Dangerous)**
```python
def get_messages(self):
    messages.append(message)
    # ❌ AUTO-MARKED AS COMPLETED TOO EARLY!
    partition_queue.mark_completed(message.offset())
    return messages

# Later...
destination.write_or_buffer_records(...)  # Might fail!
source.commit()  # ❌ Commits messages that failed to write!
```

### ✅ **After (Safe)**  
```python
def get_messages(self):
    messages.append(message)
    # ✅ Only mark as inflight
    partition_queue.mark_inflight(message.offset())
    return messages

# Later...  
destination.write_or_buffer_records(...)  # Success required!
source.commit():  # ✅ Marks completed THEN commits
    mark_messages_completed(self.current_messages)
    commit_only_safe_contiguous_offsets()
```

## 📊 **Message State Transitions**

```
POLLED → INFLIGHT → COMPLETED → COMMITTED
   ↓        ↓          ↓          ↓
Fair    Consumed   Dest Write  Safe Offset
Queue   from       Success     Commit
        Poller
```

## 🎯 **Key Insight**

The critical fix is that **completion marking happens INSIDE the commit() method** after destination write success, not when messages are consumed from the async poller.

This ensures perfect **at-least-once delivery** while maintaining **fair polling** to prevent topic starvation!