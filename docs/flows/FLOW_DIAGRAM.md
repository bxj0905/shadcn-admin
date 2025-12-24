# Flow 流程架构图

## 1. 整体架构图

```mermaid
graph TB
    subgraph "主 Flow 层"
        MainFlow[dataset-etl-flow<br/>主 ETL 流程]
        CompatFlow[rustfs-list-files<br/>兼容历史接口]
    end

    subgraph "功能 Flow 层 (Feature Flows)"
        CollectionFlow[data-collection-flow<br/>数据收集]
        ConversionFlow[data-conversion-flow<br/>数据转换<br/>待完善]
        CleaningFlow[data-cleaning-flow<br/>数据清洗<br/>待完善]
        ValidationFlow[data-validation-flow<br/>数据验证与校准]
        EncryptionFlow[data-encryption-flow<br/>数据加密<br/>待完善]
        AggregationFlow[data-aggregation-flow<br/>数据聚合<br/>待完善]
    end

    subgraph "任务层 (Tasks)"
        Task1[collect_raw_files<br/>收集原始文件 ✓]
        Task2[convert_to_parquet<br/>转换为 Parquet<br/>待完善]
        Task3[clean_data<br/>数据清洗<br/>待完善]
        Task4[upload_processed_files<br/>上传处理文件<br/>待完善]
        Task5[encrypt_sensitive_fields<br/>加密敏感字段<br/>待完善]
        Task6[aggregate_data<br/>数据聚合<br/>待完善]
    end

    subgraph "工具模块 (Utils)"
        S3Utils[s3_utils.py<br/>S3 操作工具]
        FileUtils[file_utils.py<br/>文件处理工具]
        ConfigUtils[config_utils.py<br/>配置加载工具]
    end

    subgraph "配置层 (Config)"
        ColumnTypes[column_types.yaml<br/>字段类型配置]
        SensitiveFields[sensitive_fields.yaml<br/>敏感字段配置]
        EncryptConfig[encrypt.yaml<br/>加密配置]
        OtherConfig[其他配置文件]
    end

    MainFlow --> CompatFlow
    MainFlow --> CollectionFlow
    MainFlow --> ConversionFlow
    MainFlow --> CleaningFlow
    MainFlow --> ValidationFlow
    MainFlow --> EncryptionFlow
    MainFlow --> AggregationFlow

    CollectionFlow --> Task1
    ConversionFlow --> Task2
    CleaningFlow --> Task3
    ValidationFlow --> Task3
    EncryptionFlow --> Task5
    AggregationFlow --> Task6
    CleaningFlow --> Task4

    Task1 --> S3Utils
    Task2 --> FileUtils
    Task2 --> S3Utils
    Task3 --> FileUtils
    Task4 --> S3Utils
    Task5 --> FileUtils
    Task6 --> FileUtils

    ConfigUtils --> ColumnTypes
    ConfigUtils --> SensitiveFields
    ConfigUtils --> EncryptConfig
    ConfigUtils --> OtherConfig

    Task1 --> ConfigUtils
    Task3 --> ConfigUtils
    Task5 --> ConfigUtils

    style MainFlow fill:#4CAF50,stroke:#2E7D32,color:#fff
    style CollectionFlow fill:#2196F3,stroke:#1565C0,color:#fff
    style Task1 fill:#FF9800,stroke:#E65100,color:#fff
    style ConversionFlow fill:#9E9E9E,stroke:#424242,color:#fff
    style CleaningFlow fill:#9E9E9E,stroke:#424242,color:#fff
    style EncryptionFlow fill:#9E9E9E,stroke:#424242,color:#fff
    style AggregationFlow fill:#9E9E9E,stroke:#424242,color:#fff
    style Task2 fill:#9E9E9E,stroke:#424242,color:#fff
    style Task3 fill:#9E9E9E,stroke:#424242,color:#fff
    style Task4 fill:#9E9E9E,stroke:#424242,color:#fff
    style Task5 fill:#9E9E9E,stroke:#424242,color:#fff
    style Task6 fill:#9E9E9E,stroke:#424242,color:#fff
```

## 2. 执行流程图（数据流转）

```mermaid
flowchart TD
    Start([开始: 传入 prefix<br/>例如: team-1/dataset-2/sourcedata/]) --> MainFlow

    MainFlow[dataset-etl-flow<br/>主流程入口] --> Step1

    Step1[步骤1: data-collection-flow<br/>数据收集] --> CollectTask[collect_raw_files Task]
    CollectTask --> S3List[S3 列出文件<br/>list_objects folder=prefix]
    S3List --> FilterFiles[过滤文件<br/>仅保留 .csv/.xlsx/.xls]
    FilterFiles --> UploadedFiles[生成 uploaded_files 列表<br/>包含 relative_path, target_key, bucket]
    UploadedFiles --> Step2

    Step2[步骤2: data-conversion-flow<br/>数据转换<br/>⏳ 待完善] --> ConvertTask[convert_to_parquet Task]
    ConvertTask --> ReadData[读取 CSV/Excel<br/>转换为 DataFrame]
    ReadData --> WriteParquet[写入 Parquet 格式<br/>输出到 raw_extracted 区]
    WriteParquet --> ParquetFiles[生成 parquet_files 列表]
    ParquetFiles --> Step3

    Step3[步骤3: data-cleaning-flow<br/>数据清洗<br/>⏳ 待完善] --> CleanTask[clean_data Task]
    CleanTask --> LoadConfig1[加载配置<br/>column_types.yaml]
    LoadConfig1 --> NormalizeTypes[规范数据类型<br/>DECIMAL/INTEGER/TEXT/BOOLEAN]
    NormalizeTypes --> HandleMissing[处理缺失值]
    HandleMissing --> CleanedFiles[生成 cleaned_files 列表]
    CleanedFiles --> Step3_5

    Step3_5[步骤3.5: data-validation-flow<br/>数据验证与校准] --> ValidationTask[验证逻辑]
    ValidationTask --> BuildAuth[构建权威表<br/>从 611/601 表]
    BuildAuth --> MatchCode[通过 code/name 匹配校准]
    MatchCode --> DetectIssues[检测问题<br/>code截断/一对多/缺失code]
    DetectIssues --> HasIssues{是否<br/>有问题?}
    HasIssues -->|是| WaitFix[暂停等待<br/>用户修复]
    WaitFix --> HasIssues
    HasIssues -->|否| ValidatedFiles[生成 validated_files 列表]
    ValidatedFiles --> Step4

    Step4[步骤4: data-encryption-flow<br/>数据加密<br/>⏳ 待完善] --> EncryptTask[encrypt_sensitive_fields Task]
    EncryptTask --> LoadConfig2[加载配置<br/>sensitive_fields.yaml]
    LoadConfig2 --> EncryptFields[加密敏感字段<br/>使用 SM2 加密]
    EncryptFields --> SecureFiles[生成 secure_files 列表]
    SecureFiles --> Step5

    Step5[步骤5: data-aggregation-flow<br/>数据聚合<br/>⏳ 待完善] --> AggregateTask[aggregate_data Task]
    AggregateTask --> Aggregate[数据汇总统计]
    Aggregate --> UploadTask[upload_processed_files Task]
    UploadTask --> UploadS3[上传处理后的文件到 S3]
    UploadS3 --> FinalResult[生成最终结果]

    FinalResult --> End([结束: 返回处理结果])

    style Step1 fill:#4CAF50,stroke:#2E7D32,color:#fff
    style Step2 fill:#9E9E9E,stroke:#424242,color:#fff
    style Step3 fill:#9E9E9E,stroke:#424242,color:#fff
    style Step3_5 fill:#2196F3,stroke:#1565C0,color:#fff
    style Step4 fill:#9E9E9E,stroke:#424242,color:#fff
    style Step5 fill:#9E9E9E,stroke:#424242,color:#fff
```

## 3. Flow 注册流程图

```mermaid
flowchart TD
    Start([开始注册 Flow]) --> ChooseMethod{选择注册方式}

    ChooseMethod -->|方式1| Frontend[前端界面注册<br/>data-processing 页面]
    ChooseMethod -->|方式2| BatchAPI[批量注册 API<br/>POST /api/prefect/flows/batch-register]
    ChooseMethod -->|方式3| Script[注册脚本<br/>register_flows.py]

    Frontend --> FillInfo1[填写 Flow 信息]
    FillInfo1 --> SetLabels1[设置 Labels<br/>flow_type, parent_flow, entrypoint]
    SetLabels1 --> UploadCode1[上传代码文件]
    UploadCode1 --> CreateFlow1[创建 Flow]

    BatchAPI --> PrepareFlows[准备 Flow 列表<br/>从 FLOW_REGISTRY]
    PrepareFlows --> ReadCode[读取代码文件<br/>从 file_path]
    ReadCode --> SetLabels2[批量设置 Labels]
    SetLabels2 --> CreateFlow2[批量创建 Flow]

    Script --> PrintGuide[打印注册指南]
    PrintGuide --> ShowInfo[显示 Flow 信息<br/>供手动注册参考]

    CreateFlow1 --> SetLabelsAPI[通过 Prefect API<br/>设置 Labels]
    CreateFlow2 --> SetLabelsAPI

    SetLabelsAPI --> CheckLabels{检查 Labels<br/>格式是否正确?}

    CheckLabels -->|否| Error[注册失败<br/>返回错误信息]
    CheckLabels -->|是| Success[注册成功]

    Success --> Display[前端显示 Flow<br/>根据 Labels 组织层级]

    Display --> MainFlowDisplay[主 Flow: dataset-etl-flow]
    MainFlowDisplay --> SubFlowDisplay[子 Flow 列表:<br/>- rustfs-list-files<br/>- data-collection-flow<br/>- data-conversion-flow<br/>- data-cleaning-flow<br/>- data-validation-flow<br/>- data-encryption-flow<br/>- data-aggregation-flow]

    style Frontend fill:#2196F3,stroke:#1565C0,color:#fff
    style BatchAPI fill:#4CAF50,stroke:#2E7D32,color:#fff
    style Script fill:#FF9800,stroke:#E65100,color:#fff
    style Success fill:#4CAF50,stroke:#2E7D32,color:#fff
    style Error fill:#F44336,stroke:#C62828,color:#fff
```

## 4. Labels 层级关系图

```mermaid
graph TB
    subgraph "Labels 结构说明"
        Label1["主 Flow Labels:<br/>{<br/>  'flow_type': 'main',<br/>  'entrypoint': 'docs.flows.main:dataset_etl_flow',<br/>  'description': '数据集 ETL 处理主流程'<br/>}"]
        
        Label2["功能 Flow Labels:<br/>{<br/>  'flow_type': 'feature',<br/>  'parent_flow': 'dataset-etl-flow',<br/>  'entrypoint': 'docs.flows.feature_flows.xxx',<br/>  'description': 'xxx功能 Flow'<br/>}"]
        
        Label3["子 Flow Labels:<br/>{<br/>  'flow_type': 'subflow',<br/>  'parent_flow': 'dataset-etl-flow',<br/>  'entrypoint': 'docs.flows.main:list_files_flow',<br/>  'description': '兼容历史接口的入口 Flow'<br/>}"]
    end

    subgraph "前端识别逻辑"
        Identify1[识别 flow_type === 'main'<br/>→ 主 Flow]
        Identify2[识别 flow_type === 'feature'<br/>→ 功能 Flow]
        Identify3[识别 flow_type === 'subflow'<br/>→ 子 Flow]
        MatchParent[通过 parent_flow 匹配父 Flow<br/>建立层级关系]
    end

    Label1 --> Identify1
    Label2 --> Identify2
    Label3 --> Identify3
    Identify2 --> MatchParent
    Identify3 --> MatchParent
    Identify1 --> MatchParent

    style Label1 fill:#4CAF50,stroke:#2E7D32,color:#fff
    style Label2 fill:#2196F3,stroke:#1565C0,color:#fff
    style Label3 fill:#FF9800,stroke:#E65100,color:#fff
```

## 5. 配置文件使用流程图

```mermaid
flowchart TD
    Start([需要加载配置]) --> ConfigType{配置类型}

    ConfigType -->|字段类型| ColumnTypes[column_types.yaml]
    ConfigType -->|敏感字段| SensitiveFields[sensitive_fields.yaml]
    ConfigType -->|加密配置| EncryptConfig[encrypt.yaml]

    ColumnTypes --> Load1[config_utils.load_sql_column_types]
    SensitiveFields --> Load2[config_utils.load_sensitive_fields]
    EncryptConfig --> Load3[从 encrypt.yaml 读取]

    Load1 --> TryS31[尝试从 S3 读取<br/>dataflow/config/column_types.yaml]
    Load2 --> TryS32[尝试从 S3 读取<br/>dataflow/config/sensitive_fields.yaml]

    TryS31 --> Success1{读取成功?}
    TryS32 --> Success2{读取成功?}

    Success1 -->|是| UseS3Config1[使用 S3 配置]
    Success1 -->|否| UseDefault1[使用默认配置<br/>_get_default_column_types]
    
    Success2 -->|是| UseS3Config2[使用 S3 配置]
    Success2 -->|否| UseDefault2[使用默认配置<br/>_get_default_sensitive_fields]

    UseS3Config1 --> Return1[返回配置字典]
    UseDefault1 --> Return1
    UseS3Config2 --> Return2[返回字段列表]
    UseDefault2 --> Return2

    Return1 --> Task1[任务使用配置<br/>clean_data, convert_to_parquet]
    Return2 --> Task2[任务使用配置<br/>encrypt_sensitive_fields]

    style ColumnTypes fill:#2196F3,stroke:#1565C0,color:#fff
    style SensitiveFields fill:#F44336,stroke:#C62828,color:#fff
    style EncryptConfig fill:#FF9800,stroke:#E65100,color:#fff
    style UseS3Config1 fill:#4CAF50,stroke:#2E7D32,color:#fff
    style UseS3Config2 fill:#4CAF50,stroke:#2E7D32,color:#fff
    style UseDefault1 fill:#9E9E9E,stroke:#424242,color:#fff
    style UseDefault2 fill:#9E9E9E,stroke:#424242,color:#fff
```

## 6. 模块依赖关系图

```mermaid
graph LR
    subgraph "主入口"
        Main[main.py]
        Init[__init__.py]
    end

    subgraph "功能 Flow"
        F1[data_collection_flow.py]
        F2[data_conversion_flow.py]
        F3[data_cleaning_flow.py]
        F4[data_validation_flow.py]
        F5[data_encryption_flow.py]
        F6[data_aggregation_flow.py]
    end

    subgraph "任务"
        T1[collect_raw_files.py]
        T2[convert_to_parquet.py]
        T3[clean_data.py]
        T4[upload_processed_files.py]
        T5[encrypt_sensitive_fields.py]
        T6[aggregate_data.py]
    end

    subgraph "工具"
        U1[s3_utils.py]
        U2[file_utils.py]
        U3[config_utils.py]
    end

    Main --> F1
    Main --> F2
    Main --> F3
    Main --> F4
    Main --> F5
    Main --> F6
    Init --> Main

    F1 --> T1
    F2 --> T2
    F3 --> T3
    F4 --> T3
    F5 --> T5
    F6 --> T6
    F3 --> T4

    T1 --> U1
    T2 --> U1
    T2 --> U2
    T3 --> U2
    T3 --> U3
    T4 --> U1
    T5 --> U2
    T5 --> U3
    T6 --> U2

    style Main fill:#4CAF50,stroke:#2E7D32,color:#fff
    style F1 fill:#2196F3,stroke:#1565C0,color:#fff
    style T1 fill:#FF9800,stroke:#E65100,color:#fff
```

## 说明

### 状态说明
- ✅ **绿色**: 已实现
- ⏳ **灰色**: 待完善（代码框架已创建，需要迁移实现）
- 🔵 **蓝色**: 部分实现或核心功能

### 关键概念

1. **主 Flow**: 整个 ETL 流程的入口，协调所有功能 Flow
2. **功能 Flow**: 将相关的任务组合成完整的功能单元
3. **任务 (Task)**: 最小的执行单元，负责单一职责
4. **工具模块**: 可复用的工具函数，被任务调用
5. **配置层**: 外部配置文件，支持动态加载和默认值回退

### 数据流转

```
prefix (输入)
  ↓
uploaded_files (收集的文件列表)
  ↓
parquet_files (Parquet 格式文件列表)
  ↓
cleaned_files (清洗后的文件列表)
  ↓
validated_files (验证后的文件列表)
  ↓
secure_files (加密后的文件列表)
  ↓
最终结果 (聚合统计结果)
```

### 配置加载优先级

1. **S3 配置**: 从 `dataflow/config/` 读取（优先级最高）
2. **默认配置**: 如果 S3 配置不存在，使用代码内嵌的默认配置

