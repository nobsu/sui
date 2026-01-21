# 方案二：Deterministic Batch Ordering

> 通过时间窗口批处理 + 确定性排序，减少 Shared Object 交易延迟

---

## 1. 概述

### 1.1 目标
- 针对现有 Shared Objects
- 时间窗口批处理 + 确定性排序
- 目标延迟：~200-300ms（比传统共识快 10 倍）

### 1.2 核心思想

```
传统 Consensus Path:

Client → Validators → Consensus (多轮 BFT) → Execution
                            ↓
                    多轮消息交换
                            ↓
                      延迟 2-3s

Deterministic Batch Ordering:

Client → Validators (窗口收集) → 确定性排序 → 并行执行 → 2f+1 签名
              ↓                      ↓
        等待窗口结束            独立计算顺序
        (50-100ms)             (无需协调)
              ↓
        总延迟 ~200-300ms
```

### 1.3 为什么能减少延迟？

传统共识延迟来源：
- 多轮 BFT 消息交换（500-2000ms）
- 等待区块填充
- 反压力机制

Batch Ordering 优化：
- **无需多轮协调**：所有验证者使用相同规则独立排序
- **确定性**：相同输入 → 相同顺序
- **并行验证**：验证者并行执行并签名

---

## 2. 批处理设计

### 2.1 时间窗口

```
时间线:
│ 窗口 N        │ 窗口 N+1      │ 窗口 N+2      │
├───────────────┼───────────────┼───────────────┤
│ [收集交易]    │ [收集交易]    │ [收集交易]    │
│               │               │               │
└──[处理]───────└──[处理]───────└──[处理]───────

窗口大小: 50-100ms (可配置)
```

**窗口参数**：
```rust
pub struct BatchWindowConfig {
    /// 窗口大小（毫秒）
    pub window_size_ms: u64,  // 默认: 100ms

    /// 最小批次大小（交易数）
    pub min_batch_size: usize,  // 默认: 1

    /// 最大批次大小（交易数）
    pub max_batch_size: usize,  // 默认: 10000

    /// 窗口同步容差（毫秒）
    pub sync_tolerance_ms: u64,  // 默认: 10ms
}
```

### 2.2 窗口边界同步

所有验证者需要对窗口边界达成一致：

```rust
/// 计算交易所属窗口
fn get_window_id(timestamp: u64, window_size_ms: u64) -> u64 {
    timestamp / window_size_ms
}

/// 窗口边界时间
fn get_window_start(window_id: u64, window_size_ms: u64) -> u64 {
    window_id * window_size_ms
}

fn get_window_end(window_id: u64, window_size_ms: u64) -> u64 {
    (window_id + 1) * window_size_ms
}
```

**时钟同步**：
- 依赖 NTP 或类似机制
- 容差范围内的交易归入同一窗口
- 边界交易使用交易时间戳决定

### 2.3 交易收集

```rust
// consensus_adapter.rs

pub struct BatchCollector {
    /// 当前窗口的交易
    pending_transactions: Vec<VerifiedTransaction>,

    /// 当前窗口 ID
    current_window: u64,

    /// 窗口配置
    config: BatchWindowConfig,
}

impl BatchCollector {
    /// 添加交易到当前批次
    pub fn add_transaction(&mut self, tx: VerifiedTransaction) {
        let tx_window = get_window_id(tx.timestamp(), self.config.window_size_ms);

        if tx_window == self.current_window {
            self.pending_transactions.push(tx);
        } else if tx_window > self.current_window {
            // 新窗口，先处理旧批次
            self.process_current_batch();
            self.current_window = tx_window;
            self.pending_transactions.push(tx);
        }
        // 过期交易丢弃
    }

    /// 窗口结束时处理批次
    pub fn on_window_end(&mut self) -> Vec<VerifiedTransaction> {
        let batch = std::mem::take(&mut self.pending_transactions);
        self.current_window += 1;
        batch
    }
}
```

---

## 3. 关键挑战：批次一致性问题

### 3.1 问题描述

**核心问题**：不同验证者在同一时间窗口内收集到的交易集合可能不同。

```
网络拓扑示意：

Client A ──→ [T1] ──→ Validator 1 (收到 T1, T2, T3)
                  ╲
Client B ──→ [T2] ──→ Validator 2 (收到 T1, T2, T4)  ← T3 延迟到达
                  ╱
Client C ──→ [T3] ──→ Validator 3 (收到 T1, T3, T4)  ← T2 延迟到达
                  ╲
Client D ──→ [T4] ──→ Validator 4 (收到 T2, T3, T4)  ← T1 延迟到达

窗口 N 结束时：
- Validator 1: sort({T1, T2, T3}) → [T1, T2, T3]
- Validator 2: sort({T1, T2, T4}) → [T1, T2, T4]  ← 不一致！
- Validator 3: sort({T1, T3, T4}) → [T1, T3, T4]  ← 不一致！
- Validator 4: sort({T2, T3, T4}) → [T2, T3, T4]  ← 不一致！
```

**结论**：即使排序规则确定，不同的输入集合会产生不同的输出。**确定性排序无法解决批次内容不一致的问题**。

### 3.2 可能的解决方案

#### 方案 A：两阶段批次确定

```
Phase 1: 交易广播 (窗口内)
┌────────────┐    广播收到的交易    ┌────────────┐
│ Validator  │ ←─────────────────→ │ Validator  │
│     1      │                     │     2      │
└────────────┘                     └────────────┘

Phase 2: 批次确定 (窗口结束)
- 计算交集：只处理 2f+1 个验证者都收到的交易
- 或使用并集 + 投票确定最终集合

延迟影响：+50-100ms（额外一轮广播）
```

**问题**：增加了网络通信，本质上是"轻量级共识"

#### 方案 B：Leader 模式

```
窗口 N:
┌────────────┐
│  Leader    │ ←── 收集交易
│ (轮换)     │ ──→ 广播批次内容
└────────────┘
      ↓
┌────────────┐
│ Followers  │ ←── 接受 Leader 的批次
│            │ ──→ 验证 & 执行
└────────────┘

Leader 选举：基于 epoch 和窗口 ID 确定性轮换
```

**问题**：引入中心化，Leader 可操纵顺序

#### 方案 C：延迟确认窗口

```
时间线:
│ 窗口 N (收集) │ 窗口 N+1 (传播) │ 窗口 N+2 (处理) │
├───────────────┼─────────────────┼─────────────────┤
│ 收集交易      │ 交易传播到所有   │ 处理窗口 N 的    │
│               │ 验证者          │ 交易            │

延迟影响：+100-200ms（等待传播）
```

**问题**：进一步增加延迟

### 3.3 方案评估

| 方案 | 一致性 | 延迟 | 去中心化 | 复杂度 |
|------|--------|------|---------|--------|
| 无处理（原方案） | ❌ | ~200ms | ✅ | 低 |
| 两阶段批次确定 | ✅ | ~300ms | ✅ | 中 |
| Leader 模式 | ✅ | ~200ms | ❌ | 中 |
| 延迟确认窗口 | ✅ | ~400ms | ✅ | 低 |

### 3.4 推荐方案：延迟确认窗口

考虑到去中心化要求，推荐使用"延迟确认窗口"：

```rust
pub struct DelayedBatchConfig {
    /// 收集窗口大小
    pub collection_window_ms: u64,  // 100ms

    /// 传播缓冲时间（等待交易到达所有验证者）
    pub propagation_buffer_ms: u64,  // 100ms

    /// 总延迟 = collection_window + propagation_buffer + processing
    /// ≈ 100ms + 100ms + 50ms = 250ms
}

// 处理流程
fn process_with_delay(window_id: u64) {
    // 当前时间在窗口 N+2，处理窗口 N 的交易
    let target_window = window_id - 2;
    let transactions = get_transactions_for_window(target_window);

    // 此时所有验证者应该已收到相同的交易集合
    deterministic_order(&mut transactions);
    execute_batch(transactions);
}
```

**权衡**：
- 延迟从 ~200ms 增加到 ~300-400ms
- 仍然比传统共识（~2-3s）快 5-10 倍
- 保持完全去中心化

### 3.5 与 FastShare 方案对比

| 维度 | Batch Ordering (修正后) | FastShare |
|------|------------------------|-----------|
| 批次一致性 | 需要额外机制保证 | 不需要（乐观执行） |
| 延迟 | ~300-400ms | ~400ms |
| 复杂度 | 较高 | 较低 |
| 与 Owned 混合 | 不支持 | **支持** |

**结论**：修正后的 Batch Ordering 延迟接近 FastShare，但不支持与 Owned Object 混合。对于需要批量处理的场景（如 DEX 订单匹配），Batch Ordering 仍有价值；对于需要 Owned + Shared 混合的场景，FastShare 是更好的选择。

---

## 4. 确定性排序规则

### 4.1 排序属性选择

**方案 A：按交易哈希排序**
```rust
fn deterministic_order_by_hash(transactions: &mut [VerifiedTransaction]) {
    transactions.sort_by_key(|tx| tx.digest());
}
```
- 优点：完全确定性，无法预测
- 缺点：对用户不公平（早提交不一定早执行）

**方案 B：按 (sender, nonce) 排序**
```rust
fn deterministic_order_by_sender_nonce(transactions: &mut [VerifiedTransaction]) {
    transactions.sort_by(|a, b| {
        match a.sender().cmp(&b.sender()) {
            Ordering::Equal => a.gas_data().nonce.cmp(&b.gas_data().nonce),
            other => other,
        }
    });
}
```
- 优点：同一用户的交易保持顺序
- 缺点：不同用户间无公平性

**方案 C：按接收时间戳排序（推荐）**
```rust
fn deterministic_order_by_timestamp(transactions: &mut [VerifiedTransaction]) {
    // 使用交易中的时间戳字段
    // 相同时间戳时使用哈希作为 tie-breaker
    transactions.sort_by(|a, b| {
        match a.timestamp().cmp(&b.timestamp()) {
            Ordering::Equal => a.digest().cmp(&b.digest()),
            other => other,
        }
    });
}
```
- 优点：先到先服务，相对公平
- 缺点：依赖时间戳准确性

### 4.2 抗 MEV 设计

**问题**：确定性排序规则公开，可能被利用进行 MEV 攻击

**缓解措施**：

1. **时间戳盲化**
```rust
// 使用提交-揭示模式
struct CommitRevealTransaction {
    commitment: Hash,  // H(tx || secret)
    // 窗口结束后揭示
    revealed_tx: Option<Transaction>,
    secret: Option<[u8; 32]>,
}
```

2. **批次内随机排序**
```rust
// 使用 VRF 生成批次随机种子
fn batch_ordering_with_vrf(
    transactions: &mut [VerifiedTransaction],
    vrf_output: &[u8; 32],
) {
    // 使用 VRF 输出作为种子
    let mut rng = ChaChaRng::from_seed(*vrf_output);
    transactions.shuffle(&mut rng);
}
```

3. **阈值加密**
```rust
// 交易加密提交，批次确定后解密
// 防止验证者在排序前看到交易内容
```

### 4.3 排序验证

```rust
// 所有验证者独立计算排序，结果必须一致
fn verify_batch_ordering(
    batch: &[VerifiedTransaction],
    claimed_order: &[TransactionDigest],
) -> bool {
    let mut sorted = batch.to_vec();
    deterministic_order(&mut sorted);

    sorted.iter()
        .map(|tx| tx.digest())
        .eq(claimed_order.iter().cloned())
}
```

---

## 4. 协议流程

### 4.1 完整流程

```
Step 1: 交易提交
┌────────┐    ┌────────────┐
│ Client │───→│ Validators │  交易带时间戳
└────────┘    └────────────┘
                   │
                   ↓
Step 2: 窗口收集 (50-100ms)
              ┌────────────┐
              │  收集交易   │  等待窗口结束
              │  到批次中   │
              └────────────┘
                   │
                   ↓
Step 3: 确定性排序
              ┌────────────┐
              │  独立排序   │  所有验证者相同规则
              │  无需协调   │
              └────────────┘
                   │
                   ↓
Step 4: 并行执行
              ┌────────────┐
              │  按序执行   │  验证者并行处理
              │  计算效果   │
              └────────────┘
                   │
                   ↓
Step 5: 签名收集
              ┌────────────┐
              │ 2f+1 签名   │  收集效果签名
              │  确认批次   │
              └────────────┘
                   │
                   ↓
Step 6: 批次确认
              ┌────────────┐
              │  广播确认   │  通知客户端
              └────────────┘
```

### 4.2 代码实现

```rust
// consensus_handler.rs

impl BatchOrderingHandler {
    /// 处理批次
    pub async fn process_batch(
        &self,
        batch: Vec<VerifiedTransaction>,
        window_id: u64,
    ) -> Result<BatchEffects, BatchError> {
        // Step 1: 确定性排序
        let mut sorted_batch = batch;
        deterministic_order(&mut sorted_batch);

        // Step 2: 按序执行
        let mut effects = Vec::new();
        for tx in &sorted_batch {
            let effect = self.execute_transaction(tx).await?;
            effects.push(effect);
        }

        // Step 3: 生成批次摘要
        let batch_digest = compute_batch_digest(&sorted_batch, &effects);

        // Step 4: 签名
        let signature = self.sign_batch(batch_digest)?;

        // Step 5: 收集 2f+1 签名
        let certificate = self.collect_signatures(batch_digest, signature).await?;

        Ok(BatchEffects {
            window_id,
            transactions: sorted_batch.iter().map(|tx| tx.digest()).collect(),
            effects,
            certificate,
        })
    }
}
```

### 4.3 跨窗口交易处理

```rust
// 交易可能因网络延迟跨越窗口边界

pub enum CrossWindowPolicy {
    /// 归入下一个窗口
    DeferToNext,

    /// 使用交易时间戳决定
    UseTransactionTimestamp,

    /// 丢弃（需要客户端重试）
    Reject,
}

fn assign_to_window(
    tx: &VerifiedTransaction,
    current_window: u64,
    policy: CrossWindowPolicy,
) -> Option<u64> {
    let tx_window = get_window_id(tx.timestamp(), WINDOW_SIZE_MS);

    if tx_window < current_window {
        // 过期交易
        match policy {
            CrossWindowPolicy::DeferToNext => Some(current_window),
            CrossWindowPolicy::UseTransactionTimestamp => None,  // 丢弃
            CrossWindowPolicy::Reject => None,
        }
    } else {
        Some(tx_window)
    }
}
```

---

## 5. 实现路径

### 5.1 Sui 源码修改点

| 模块 | 文件 | 修改内容 |
|------|------|---------|
| **交易类型** | `sui-types/src/transaction.rs` | 新增批次模式标记 |
| **批处理逻辑** | `sui-core/src/consensus_adapter.rs` | BatchCollector 实现 |
| **排序逻辑** | `sui-core/src/batch_ordering.rs` | 确定性排序算法（新文件） |
| **执行调度** | `sui-core/src/authority.rs` | 批次执行流程 |
| **签名收集** | `sui-core/src/effects_certifier.rs` | 批次签名 |

### 5.2 交易类型扩展

```rust
// sui-types/src/transaction.rs

pub struct TransactionData {
    // ... 现有字段 ...

    /// 批处理模式标记
    pub batch_mode: Option<BatchMode>,
}

pub enum BatchMode {
    /// 使用确定性批次排序
    DeterministicBatch {
        /// 期望的窗口 ID（可选）
        expected_window: Option<u64>,
    },

    /// 传统共识路径
    Consensus,
}
```

### 5.3 渐进式实现

```
Phase 1: 基础框架
├── BatchCollector 实现
├── 时间窗口管理
└── 确定性排序算法

Phase 2: 执行与签名
├── 批次执行流程
├── 批次效果聚合
└── 2f+1 签名收集

Phase 3: 协议优化
├── 跨窗口处理
├── 超时回退机制
└── 抗 MEV 措施

Phase 4: 客户端支持
├── SDK 批次模式
├── 窗口状态查询
└── 批次确认通知
```

---

## 6. 开发者指南

### 6.1 何时使用 Batch Ordering

**适合场景**：
- 批量撮合 DEX（订单匹配）
- 需要全局公平顺序
- 不需要实时确认
- 高吞吐量场景

**不适合场景**：
- 实时性要求极高（< 100ms）
- 需要立即确认
- 与 Owned Object 混合操作（使用 FastShare 方案）

### 6.2 合约设计

**批次感知设计**：
```move
module dex::order_book {
    /// 订单簿 - 适合 Batch Ordering
    struct OrderBook has key {
        id: UID,
        buy_orders: vector<Order>,
        sell_orders: vector<Order>,
    }

    /// 批量撮合
    /// 在一个批次内，所有订单按确定性顺序处理
    public fun match_orders(book: &mut OrderBook) {
        // 按价格排序
        sort_orders(&mut book.buy_orders, /* descending */);
        sort_orders(&mut book.sell_orders, /* ascending */);

        // 撮合
        while (can_match(&book.buy_orders, &book.sell_orders)) {
            let trade = execute_trade(
                &mut book.buy_orders,
                &mut book.sell_orders,
            );
            emit_trade_event(trade);
        }
    }
}
```

**幂等性保证**：
```move
/// 使用 nonce 防止重复执行
public fun submit_order(
    book: &mut OrderBook,
    order: Order,
    nonce: u64,
) {
    // 检查 nonce 是否已使用
    assert!(!is_nonce_used(book, order.owner, nonce), E_DUPLICATE_NONCE);

    // 记录 nonce
    mark_nonce_used(book, order.owner, nonce);

    // 添加订单
    add_order(book, order);
}
```

### 6.3 客户端使用

```typescript
// TypeScript SDK

import { SuiClient, Transaction } from '@mysten/sui.js';

async function submitBatchOrder(
    client: SuiClient,
    orderBook: string,
    order: OrderParams,
) {
    const tx = new Transaction();

    // 设置批处理模式
    tx.setBatchMode({
        type: 'DeterministicBatch',
        // 可选：指定期望窗口
        // expectedWindow: 12345,
    });

    tx.moveCall({
        target: 'dex::order_book::submit_order',
        arguments: [
            tx.object(orderBook),
            tx.pure(order),
        ],
    });

    // 提交交易
    const result = await client.signAndExecuteTransaction(tx);

    // 等待批次确认
    const batchConfirmation = await client.waitForBatchConfirmation(
        result.digest,
    );

    console.log(`Order included in window ${batchConfirmation.windowId}`);
    console.log(`Position in batch: ${batchConfirmation.positionInBatch}`);
}
```

---

## 7. 性能分析

### 7.1 延迟对比

| 阶段 | 传统共识 | Batch Ordering |
|------|---------|----------------|
| 交易提交 | 10ms | 10ms |
| 窗口等待 | - | **50-100ms** |
| 顺序确定 | 500-2000ms | **0ms** |
| 签名收集 | 包含在共识中 | **100-150ms** |
| 执行 | 10ms | 10ms |
| **总计** | **~2-3s** | **~200-300ms** |

### 7.2 吞吐量分析

```
窗口大小: 100ms
批次容量: 10000 交易/批次

理论最大吞吐量: 10000 / 0.1s = 100,000 TPS

实际吞吐量受限于:
- 执行速度
- 网络带宽
- 签名收集延迟
```

### 7.3 窗口大小权衡

| 窗口大小 | 延迟 | 吞吐量 | 公平性 |
|---------|------|--------|--------|
| 50ms | ~150ms | 中 | 较好 |
| 100ms | ~200ms | 高 | 好 |
| 200ms | ~300ms | 很高 | 很好 |
| 500ms | ~600ms | 极高 | 最好 |

---

## 8. 局限性与权衡

### 8.1 局限性

1. **窗口等待延迟**
   - 最少等待半个窗口时间
   - 不适合需要立即确认的场景

2. **时钟同步依赖**
   - 验证者需要时钟同步
   - 时钟偏差可能导致交易归入不同窗口

3. **不支持 Owned 混合**
   - 纯 Shared Object 交易
   - 需要 Owned 混合时使用 FastShare 方案

4. **MEV 风险**
   - 排序规则公开
   - 需要额外措施防止 MEV

### 8.2 设计权衡

| 权衡点 | Batch Ordering 选择 | 替代方案 |
|--------|---------------------|---------|
| 延迟 vs 公平 | 窗口批处理 | 即时处理 |
| 简单性 vs 灵活性 | 固定窗口 | 动态窗口 |
| 去中心化 vs 延迟 | 确定性排序 | 单点排序 |

---

## 9. 与方案一的对比

参见 [FASTSHARE_UNIFIED_PATH.md](./FASTSHARE_UNIFIED_PATH.md)

| 维度 | Batch Ordering | FastShare + Unified Path |
|------|----------------|-------------------------|
| 延迟 | ~200-300ms | ~400ms |
| 适用对象 | 现有 Shared Objects | 新 FastShare 类型 |
| 与 Owned 混合 | 不支持 | **支持** |
| 冲突处理 | 批内排序 | 回滚重试 |
| 实现复杂度 | **低** | 中 |
| 兼容性 | **现有合约可用** | 需要新合约 |

### 选择建议

- **选择 Batch Ordering**：批量撮合场景、需要公平排序、现有合约
- **选择 FastShare**：需要与 Owned 混合、低冲突场景、新开发合约
