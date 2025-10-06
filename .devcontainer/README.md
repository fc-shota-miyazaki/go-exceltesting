# Go Excel Testing DevContainer

このプロジェクト用のDevContainer設定です。

## 含まれる機能

- **Go 1.19**: プロジェクトで使用されているGoのバージョン
- **Docker Compose**: PostgreSQLとdevcontainerが連携して起動
- **VS Code拡張機能**:
  - Go拡張機能
  - JSON/YAMLサポート
  - Dockerサポート
- **PostgreSQL**: ポート15432でPostgreSQLデータベースが利用可能
- **環境変数**: `EXCELTESTING_CONNECTION`が自動設定

## 使用方法

1. VS Codeでこのプロジェクトを開く
2. コマンドパレット（Cmd+Shift+P）で「Dev Containers: Reopen in Container」を選択
3. Docker ComposeでPostgreSQLとdevcontainerが同時に起動します

## データベース接続

DevContainer内では以下の接続文字列でPostgreSQLに接続できます：

```
postgres://excellocal:password@postgres:5432/excellocal?sslmode=disable
```

## テスト実行

DevContainer内でテストを実行：

```bash
go test ./...
```

## CLIツールの使用

```bash
# Excelテンプレートの生成
exceltesting dump out.xlsx

# データベースとExcelファイルの比較
exceltesting compare testdata/compare.xlsx

# テストデータの読み込み
exceltesting load testdata/load.xlsx
```

## Docker Compose構成

- `postgres`: PostgreSQL 14.3データベース
- `devcontainer`: Go 1.19開発環境

devcontainerはPostgreSQLの起動完了を待ってから起動します。
