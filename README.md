# Attribute Filters

- Returns visible item attributes and options when a category is selected
- Corrects `search_conditions` to remove invalid selections

## Usage

```bash
#
# Find all visible attributes given the following search conditions:
#
#   category_id: 242,         // レディース - 小物 - 折り財布
#   attributes: [
#     {
#        attribute_id: 1893,  // ブランド
#        option_id:    45716  // - シャネル
#     },
#     {
#        attribute_id: 1212,  // 素材/種類
#        option_id:    175115 // リザード・エキゾチック系
#     },
#   ]
#
$ go run cmd/find/main.go -d ../test-data -c ../test-data/categories.json --cid 242 -a 1893-45716,1212-175115

Loaded 1337 categories
Reading CSV records from file: attribute.csv.gz
Read 7741 records total
Reading CSV records from file: attribute_option.csv.gz
Read 173819 records total
Reading CSV records from file: category_attribute.csv.gz
Read 8977 records total
Reading CSV records from file: dynamic_attribute_option.csv.gz
Read 73672 records total
Finished loading data in 316.62776ms
Finished post-processing data in 50.877071ms

=== Stats ===
Attributes        : 7741
Options           : 173818
Category Rules    : 1145
Refs              : 814374

=== Result ===

 - attribute [4559  ] - カラー
    - option [10106 ] - ブルー
    - option [10103 ] - ブラウン
    - option [10095 ] - グレー
 - attribute [1893  ] - ブランド
    - option [158069] - マネブ
    - option [37828 ] - ジョンソンズ
    - option [83085 ] - チャージフェイスマスク
 - attribute [2619  ] - 財布形/開け口
    - option [10127 ] - がま口
    - option [10117 ] - 三つ折り
    - option [10124 ] - 二つ折り
 - attribute [1355  ] - 色
    - option [177160] - シルバー系
    - option [177151] - ベージュ系
    - option [177147] - ホワイト系
 - attribute [1356  ] - 付属品
    - option [177162] - 箱（袋）あり / ギャランティーあり
    - option [177164] - 箱（袋）なし / ギャランティーあり
    - option [177163] - 箱（袋）あり / ギャランティーなし
 - attribute [1211  ] - 型名
    - option [175094] - マトラッセ
    - option [175095] - ココハンドル
    - option [175096] - ヴァニティ
    - option [175097] - チョコバー
    - option [175098] - カプシーヌ
    - option [175099] - カンボンライン
    - option [175100] - コココクーン
    - option [175101] - エグゼクティブ
    - option [175102] - ドーヴィル
    - option [175103] - クルーズライン
    - option [175104] - パリビアリッツ
    - option [175105] - オンザロード
    - option [175106] - ラパン
    - option [175107] - ココマーク
    - option [175108] - バックパック
    - option [175109] - ガブリエルドゥ
    - option [175110] - チェーンウォレット
    - option [175111] - マトラッセ タッセル
    - option [175112] - ラフィア
    - option [175113] - その他
 - attribute [1212  ] - 素材/種類
    - option [175114] - レザー・ファー系
    - option [175115] - リザード・エキゾチック系
    - option [175116] - コットン系・ナイロン系・その他
 - attribute [1213  ] - 素材/詳細
    - option [175124] - クロコダイル
    - option [175125] - パイソン
    - option [175126] - リザード
    - option [175127] - ガルーシャ

```
