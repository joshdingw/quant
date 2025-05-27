# 量化交易项目结构文档

## 项目概述
这是一个量化交易项目，包含数据获取、策略实现、数据库管理等功能模块。

## 目录结构

### 根目录
- `requirements.txt`: 项目依赖包列表
- `README.md`: 项目说明文档
- `.gitignore`: Git忽略文件配置

### utils/ - 工具类模块
- `data_fetcher.py`: 数据获取工具
- `tushare_loader.py`: Tushare数据加载器
- `db_manager.py`: 数据库管理工具
- `db_initializer.py`: 数据库初始化工具
- `__init__.py`: 包初始化文件

### Strategy/ - 策略模块
- `MA.ipynb`: 移动平均策略实现

### Database/ - 数据库模块
- `history.db`: SQLite数据库文件，存储历史数据
- `test.db`: SQLite数据库文件，用于单元测试

### Config/ - 配置模块
- `config.yaml`: 项目配置文件

### Unittest/ - 单元测试模块
- `data_fetcher_test.py`: 数据获取模块的单元测试
- `temptest.py`: 测试文件

### Notebooks/ - Jupyter笔记本目录
用于开发和测试策略的Jupyter笔记本

## 主要功能模块

### 数据获取模块
- 通过`data_fetcher.py`和`tushare_loader.py`实现数据获取功能
- 支持从Tushare获取股票数据

### 数据库管理
- 使用SQLite数据库存储历史数据
- 通过`db_manager.py`提供数据库操作接口
- 使用`db_initializer.py`统一管理数据库初始化
  - 支持同时初始化历史数据库(history.db)和测试数据库(test.db)
  - 确保数据库结构的一致性
  - 在项目启动时自动完成初始化

### 策略实现
- 在Strategy目录下实现交易策略
- 目前包含移动平均策略(MA)

### 配置管理
- 使用YAML格式的配置文件管理项目参数

### 测试框架
- 使用Python的unittest框架进行单元测试
- 测试数据库(test.db)与历史数据库(history.db)保持相同的结构
- 测试用例会自动清理测试数据，确保测试环境的独立性

## 开发环境
- Python项目
- 使用Jupyter Notebook进行策略开发和测试
- 包含单元测试框架