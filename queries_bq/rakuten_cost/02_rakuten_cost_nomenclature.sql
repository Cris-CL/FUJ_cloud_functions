-- Account mapping for the cost lines of the 楽天ペイ Billpay statement, read by
-- FUJ_scheduled.rakuten_cost_report(). Seeded from Google Sheet RAK_PAYMENT, tab
-- cost_nomenclature (36 rows, as provided by the owner on 2026-10-16) plus one half-width bracket variant (row 37).
-- The INSERT only adds subject names that are not in the table yet, so the file can be re-run.
-- To add a new 品目: INSERT one row by hand (same columns) when the report shows MISSING NOMENCLATURE.
CREATE TABLE IF NOT EXISTS `test-bigquery-cc.Rakuten.cost_nomenclature`
(
  subject_name    STRING   OPTIONS (description = '品目 of the Billpay statement (集約科目名 in older files). Exact match: half-width katakana and mixed bracket widths are intentional'),
  remarks         STRING   OPTIONS (description = '-> 備考'),
  item            STRING   OPTIONS (description = '-> 品目 (freee)'),
  tax_distinction STRING   OPTIONS (description = '-> 税区分; 対象外 means no 10% tax is added'),
  account         STRING   OPTIONS (description = '-> 勘定科目'),
  translation     STRING   OPTIONS (description = 'English translation of subject_name (reference only)'),
  ADDED_DATETIME  DATETIME OPTIONS (description = 'When the row was added')
)
OPTIONS (
  description = 'Account mapping for 楽天ペイ statement cost lines (Rakuten.statement.subject_name -> 勘定科目 / 税区分 / 備考 / 品目). Single source of truth; add rows by hand when FUJ_scheduled.rakuten_cost_report() shows MISSING NOMENCLATURE.'
);

INSERT INTO `test-bigquery-cc.Rakuten.cost_nomenclature`
  (subject_name, remarks, item, tax_distinction, account, translation, ADDED_DATETIME)
SELECT
  seed.subject_name, seed.remarks, seed.item, seed.tax_distinction, seed.account, seed.translation,
  CURRENT_DATETIME()
FROM UNNEST(ARRAY<STRUCT<subject_name STRING, remarks STRING, item STRING, tax_distinction STRING, account STRING, translation STRING>>[
  ('ｼｽﾃﾑ利用料_PC', 'System Usage', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'System usage fee_PC'),
  ('ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_成果報酬原資', 'Achievement Reward Fund', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Super Affiliate - Source of Performance-Based Rewards'),
  ('ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾌｨﾘｴｲﾄｼｽﾃﾑ利用料', 'Affiliate System', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Super Affiliate Affiliate System Usage Fee'),
  ('ﾌﾟﾗﾝ共通_ﾎﾟｲﾝﾄ付与料_PC', 'Point Grant', 'Fee Rakuten', '対象外', '支払手数料', 'Plan Common_Point Granting Fee_PC'),
  ('ﾌﾟﾗﾝ共通_ﾎﾟｲﾝﾄ付与料_ﾓﾊﾞｲﾙ', 'Point Grant Mobile', 'Fee Rakuten', '対象外', '支払手数料', 'Plan Common_Point Granting Fee_Mobile'),
  ('ﾌﾟﾗﾝ共通_ﾓｰﾙにおける取引の安全性･利便性向上のためのｼｽﾃﾑ利用料', 'Affiliate System Usage', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Common to all plans: System usage fee for improving the security and convenience of transactions in the mall.'),
  ('ﾌﾟﾗﾝ共通_楽天ﾍﾟｲ利用料', 'Rakuten Pay', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Plan Common Rakuten Pay Usage Fee'),
  ('ｼｽﾃﾑ利用料_ﾓﾊﾞｲﾙ', 'System Usage Mobile', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'System usage fee_mobile'),
  ('ﾌﾟﾗﾝ共通_商品一括登録ｻｰﾋﾞｽ', 'Product Batch Registration', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Plan-wide bulk product registration service'),
  ('広告掲載料_楽天CPA広告', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Advertising fees - Rakuten CPA advertising'),
  ('広告掲載料_楽天ﾌﾟﾛﾓｰｼｮﾝﾌﾟﾗｯﾄﾌｫｰﾑ', 'Advertisement', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Advertising fees - Rakuten Promotion Platform'),
  ('検索連動型広告(RPP)_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Search engine marketing (RPP) - Ad placement fees'),
  ('効果保証型広告(楽天CPA広告)_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Performance-guaranteed advertising (Rakuten CPA advertising) - Advertising fees'),
  ('消費税', NULL, NULL, NULL, NULL, 'consumption tax'),
  ('ｽﾀﾝﾀﾞｰﾄﾞﾌﾟﾗﾝ_出店料', 'Initial Fee Shop Setting', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Standard Plan - Booth Fee'),
  ('R-Mail_配信料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'R-Mail_delivery fee'),
  ('運用型ｸｰﾎﾟﾝ広告(ｸｰﾎﾟﾝｱﾄﾞﾊﾞﾝｽ広告)_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Programmatic coupon advertising (Coupon Advance advertising) - Advertising fees'),
  ('検索連動型広告（RPP）_控除分', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Search engine marketing (RPP) deduction'),
  ('R-Mail_配信料_無料配信適用額', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'R-Mail_Distribution Fee_Free Distribution Applicable Amount'),
  ('ｽｰﾊﾟｰDEAL_広告掲載料_ﾓﾊﾞｲﾙ', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Super Deal Advertisement Fees - Mobile'),
  ('ｽｰﾊﾟｰDEAL_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Super Deal - Advertising Fees'),
  ('ｽｰﾊﾟｰDEAL_広告掲載料_ｸｰﾎﾟﾝ控除額', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Super Deal Advertisement Fee - Coupon Deduction Amount'),
  ('楽天市場／楽天ｸﾞﾙｰﾌﾟ広告_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Rakuten Market / Rakuten Group Advertising - Advertising Fees'),
  ('楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額', 'Fulfillment Fee', 'Delivery', '課対仕入10%', '荷造運賃', 'Rakuten Super Logistics Service Fees - Taxable Amount'),
  ('楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額【割引】', 'Fulfillment Fee Discount', 'Delivery', '課対仕入10%', '荷造運賃', 'Rakuten Super Logistics Service Fees - Taxable Amount [Discount]'),
  ('楽天ｽｰﾊﾟｰﾛｼﾞｽﾃｨｸｽ_ｻｰﾋﾞｽ料金_課税対象額（返還）', 'Fulfillment Fee Refund', 'Delivery', '課対仕入10%', '荷造運賃', 'Rakuten Super Logistics Service Fees - Taxable Amount (Refund)'),
  ('ｻｰﾋﾞｽｽｸｴｱ_BOSS_ｻｰﾋﾞｽ利用料', 'Fulfillment Fee', 'Delivery', '課対仕入10%', '荷造運賃', 'Service Square BOSS Service Usage Fee'),
  ('出店料_差額請求', 'Initial Fee Shop Setting', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Booth fee - difference billing'),
  ('R-SNS_月額固定費', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'R-SNS_Monthly fixed cost'),
  ('ｼｽﾃﾑ利用料_ｼｮｯﾌﾟｸｰﾎﾟﾝ', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'System usage fee_shop coupon'),
  ('ｼｽﾃﾑ利用料_ｼｮｯﾌﾟｸｰﾎﾟﾝ_値引き', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'System usage fee_Shop coupon_Discount'),
  ('ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾄﾞﾊﾞﾝｽｻｰﾋﾞｽ料', 'Affiliate System', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Super Affiliate Advanced Service Fee'),
  ('ｽｰﾊﾟｰｱﾌｨﾘｴｲﾄ_ｱﾄﾞﾊﾞﾝｽｻｰﾋﾞｽ料【割引】', 'Affiliate System', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Super Affiliate Advanced Service Fee [Discount]'),
  ('定期購入･頒布会購入_ｼｽﾃﾑ利用料', 'System Usage', 'Fee Rakuten', '課対仕入10%', '支払手数料', 'Subscription/Regular Purchase System Usage Fee'),
  ('R-SNS_LINE ﾗｲﾄﾌﾟﾗﾝ_月額費用', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'R-SNS_LINE Light Plan_Monthly Fee'),
  ('検索連動型広告-ｴｸｽﾊﾟﾝｼｮﾝ(RPP-EXP)_広告掲載料', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Search engine advertising - Expansion (RPP-EXP) - Ad placement fees'),
  -- row 37: half-width bracket variant of row 18, as written in the 2026 statements; the sheet mapped it the same way (3 rows in cost_calc)
  ('検索連動型広告(RPP)_控除分', 'Advertisement', 'Advertisement', '課対仕入10%', '広告宣伝費', 'Search engine marketing (RPP) deduction (half-width brackets)')
]) AS seed
WHERE seed.subject_name NOT IN (
  SELECT subject_name FROM `test-bigquery-cc.Rakuten.cost_nomenclature` WHERE subject_name IS NOT NULL
);
