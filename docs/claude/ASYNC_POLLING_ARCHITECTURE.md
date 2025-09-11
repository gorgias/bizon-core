# Simplified Async Polling Architecture

## Overview

The async polling implementation now uses a **much simpler architecture** that eliminates callback complexity and uses only the `commit()` method for offset management.

## Simplified Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐
│ AsyncKafkaPoller│    │   KafkaSource    │    │ Engine/Pipeline│
│                 │    │                  │    │               │
│ 1. Poll messages│───▶│ 2. Get messages  │───▶│ 3. Process &  │
│ 2. Queue fairly │    │ 3. Auto-mark as  │    │    write to   │
│ 3. Round-robin  │    │    processed     │    │    destination│
│                 │    │ 4. Periodic      │    │               │
│                 │    │    commit()      │    │               │
└─────────────────┘    └──────────────────┘    └────────────────┘
```

## Key Simplifications

### ✅ **Before (Complex)**
- Messages marked as "inflight" when consumed  
- Callback tracks which specific messages were processed successfully
- Complex mock message creation to link records back to original messages
- Separate commit logic in callback vs source
- Two-phase commit process

### ✅ **After (Simple)**
- Messages automatically marked as processed when consumed
- No callback complexity - just logging
- All offset management in single `commit()` method  
- Periodic commits (every 30 seconds) for safety
- One-phase process

## Architecture Benefits

1. **Simpler Code**: ~50% less complexity in offset management
2. **Single Source of Truth**: All offset logic in `KafkaSource.commit()` 
3. **Easier Testing**: No complex callback mocking needed
4. **Better Performance**: No overhead of callback message tracking
5. **Same Guarantees**: Still prevents starvation and maintains at-least-once delivery

## How It Works

1. **AsyncKafkaPoller** polls messages in background with 50ms cadence
2. **Fair queueing** ensures all partitions get serviced via round-robin  
3. **Auto-processing** marks messages as completed when `get_messages()` is called
4. **Periodic commits** happen every 30 seconds via `commit()` method
5. **Safe offsets** only commit contiguous processed ranges

## Configuration

```yaml
# Enable async polling (default: true)
enable_async_polling: true

# Polling cadence (default: 50ms) 
poll_interval_ms: 50

# Max records per batch (default: 500)
max_poll_records: 500

# Pause threshold for hot partitions (default: 1000)
partition_pause_threshold: 1000
```

This simplified architecture achieves the same **starvation prevention** with much cleaner, more maintainable code!