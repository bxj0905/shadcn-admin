-- report_populate.sql

-- ──（可选）先补齐源表中可能缺失的列，让 UPDATE 时能至少拿到 NULL 而不是找不到列
-- ── 补齐 611 表的所有源字段
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "省（自治区、直辖市）"         TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "地（市、州、盟）"             TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "县（市、区、旗）"             TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "乡（镇、街道）"         TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "是否与单位所在地详细地址一致：" TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "省（自治区、直辖市）注册地区"   TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "地（市、州、盟）注册地区"       TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "县（市、区、旗）注册地区"       TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "乡（镇、街道）注册地区"   TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "机构类型"                   TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "登记注册统计类别"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "行业代码"                   TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "单位规模"                   TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "登记注册统计类别"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "法人单位详细名称"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "法人单位详细地址"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "法人单位区划代码"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "从业人员期末人数"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "从业人员期末人数其中女性"     TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "经营性单位收入"               DECIMAL(18,2);;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "非经营性单位支出 （费用）"    DECIMAL(18,2);;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "单位类型（最新）"             TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "主要业务活动"                 TEXT;

-- ── 补齐 601 表的所有源字段
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "省（自治区、直辖市）"            TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "地（市、州、盟）"              TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "县（区、市、旗）"              TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "乡（镇、街道）"              TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "是否与单位所在地详细地址一致"  TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "省（自治区、直辖市）注册地区"  TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "地（市、州、盟）注册地区"      TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "县（市、区、旗）注册地区"      TEXT;
ALTER TABLE IF EXISTS "611" ADD COLUMN IF NOT EXISTS "乡（镇、街道）注册地区"      TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "机构类型"                    TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "登记注册统计类别"              TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "行业代码"                    TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "单位规模"                    TEXT;
ALTER TABLE IF EXISTS "601" ADD COLUMN IF NOT EXISTS "主要业务活动"                TEXT;

-- ── 补齐 611-3/611-4/611-6/611-7 表的初始两列
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;

-- ── 补齐 B603/C603/E603/F603/S603/X603 表的初始两列
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "统一社会信用代码" TEXT;
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "单位详细名称"   TEXT;

-- ── 补齐 602/611-2 表的从业人数字段
ALTER TABLE IF EXISTS "602"   ADD COLUMN IF NOT EXISTS "从业人员期末人数;本年"  INTEGER;
ALTER TABLE IF EXISTS "602"   ADD COLUMN IF NOT EXISTS "其中：女性;本年"          INTEGER;
ALTER TABLE IF EXISTS "611-2" ADD COLUMN IF NOT EXISTS "从业人员期末人数;本年"  INTEGER;
ALTER TABLE IF EXISTS "611-2" ADD COLUMN IF NOT EXISTS "其中：女性;本年"          INTEGER;

-- ── 补齐 607-2 表的研发和专利字段
ALTER TABLE IF EXISTS "607-2" ADD COLUMN IF NOT EXISTS "研究开发人员合计;本年"  INTEGER;
ALTER TABLE IF EXISTS "607-2" ADD COLUMN IF NOT EXISTS "研究开发费用合计;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "607-2" ADD COLUMN IF NOT EXISTS "当年专利申请数;本年"    INTEGER;
ALTER TABLE IF EXISTS "607-2" ADD COLUMN IF NOT EXISTS "期末有效发明专利数;本年"  INTEGER;

-- ── 补齐 B603 表的所有更新字段
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其中：本年折旧211;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "投资收益（损失以—号记）;本年"             DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "公允价值变动收益（损失以—号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "资产处置收益（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "净敞口套期收益（损失以—号记）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "上交政府的各项非税费用;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其中：上缴的各项税费;本年"              DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其他属于劳动者报酬的部分;本年"          DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "董事会费;本年"                          DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "差旅费;本年"                            DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "职工教育经费;本年"                      DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其中：上交管理费;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "其中：利息费用;本年"                    DECIMAL(18,2);
ALTER TABLE IF EXISTS "B603" ADD COLUMN IF NOT EXISTS "利息收入;本年"                          DECIMAL(18,2);
-- ── 补齐 C603 表的所有更新字段
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "其中：本年折旧1;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "投资收益损失以—号记;本年"                DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "公允价值变动收益损失以—号记;本年"        DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "资产处置收益损失以—号记;本年"            DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "C603" ADD COLUMN IF NOT EXISTS "净敞口套期收益损失以—号记;本年"         DECIMAL(18,2);

-- ── 补齐 E603/F603/S603/X603 表的所有更新字段（同 B603 格式，X603 有额外一列）
-- E603
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "其中：本年折旧211;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "投资收益（损失以—号记）;本年"             DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "公允价值变动收益（损失以—号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "资产处置收益（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "E603" ADD COLUMN IF NOT EXISTS "净敞口套期收益（损失以—号记）;本年"       DECIMAL(18,2);
-- F603
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "其中：本年折旧211;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "资产减值损失损失以—号记;本年"            DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "信用减值损失损失以—号记;本年"            DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "投资收益损失以—号记;本年"               DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "公允价值变动收益损失以—号记;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "资产处置收益损失以—号记;本年"           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "F603" ADD COLUMN IF NOT EXISTS "净敞口套期收益损失以—号记;本年"         DECIMAL(18,2);
-- S603
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "其中：本年折旧211;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "投资收益（损失以—号记）;本年"             DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "公允价值变动收益（损失以—号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "资产处置收益（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "S603" ADD COLUMN IF NOT EXISTS "净敞口套期收益（损失以—号记）;本年"       DECIMAL(18,2);
-- X603
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "营业成本;本年"                             DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "固定资产累计折旧-其中：本年折旧;本年"    DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "资产总计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "负债合计;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "营业收入;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"       DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "营业利润;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "投资收益（损失以—号记）;本年"             DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "公允价值变动收益（损失以—号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "资产处置收益（损失以—号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "其他收益;本年"                           DECIMAL(18,2);
ALTER TABLE IF EXISTS "X603" ADD COLUMN IF NOT EXISTS "净敞口套期收益（损失以—号记）;本年"       DECIMAL(18,2);

-- ── 补齐 611-3/611-4/611-6/611-7 表的所有更新字段
-- ── 补齐 611-3 表的所有更新字段
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "营业成本;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "本年折旧;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "资产总计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "负债合计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "营业收入;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                       DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "营业利润;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "投资收益;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "其他收益;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "研究开发人员合计;本年"                 DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-3" ADD COLUMN IF NOT EXISTS "研究开发费用合计;本年"                 DECIMAL(18,2);

-- ── 补齐 611-4 表的所有更新字段
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "营业成本（营业支出）;本年"             DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "其中：本年折旧;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "资产总计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "负债合计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "营业收入;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "税金及附加;本年"                       DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "应交增值税（本年累计发生额）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "应付职工薪酬（本年贷方累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "营业利润;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "资产减值损失（损失以－号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "信用减值损失（损失以－号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "投资收益（损失以－号记）;本年"         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "公允价值变动收益（损失以－号记）;本年"   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "资产处置收益（损失以－号记）;本年"     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "其他收益;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "净敞口套期收益（损失以－号记）;本年"   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-4" ADD COLUMN IF NOT EXISTS "研发费用;本年"                         DECIMAL(18,2);

-- ── 补齐 611-6 表的所有更新字段
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "其中：本年折旧2;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "资产总计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "负债合计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "经营收入;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "税金及附加费用;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "四、应交增值税（本年累计发生额）;本年"  DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "其中：工资福利支出;本年"               DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "其中：劳务费;本年"            DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "福利费;本年"                  DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "对个人和家庭的补助;本年"      DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "工会经费;本年"      DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-6" ADD COLUMN IF NOT EXISTS "经营支出;本年"      DECIMAL(18,2);

-- ── 补齐 611-7 表的所有更新字段
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "其中：本年折旧;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "资产总计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "负债合计;本年"                         DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "税费3;本年"                            DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "税费;本年"                              DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "其中：人员费用3;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "其中：人员费用;本年"                   DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "固定资产折旧3;本年"                     DECIMAL(18,2);
ALTER TABLE IF EXISTS "611-7" ADD COLUMN IF NOT EXISTS "固定资产折旧;本年"                      DECIMAL(18,2);



-- 一 更新 RPT01表
-- 1. 数据来源记录
INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  '611-3',
  FALSE
FROM "611-3";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  '611-4',
  FALSE
FROM "611-4";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  '611-6',
  FALSE
FROM "611-6";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  '611-7',
  FALSE
FROM "611-7";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'B603',
  TRUE
FROM "B603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'C603',
  TRUE
FROM "C603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'E603',
  TRUE
FROM "E603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'F603',
  TRUE
FROM "F603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'S603',
  TRUE
FROM "S603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  'X603',
  TRUE
FROM "X603";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  "数据来源",
  "是否一套表单位"
FROM "temp601";

INSERT INTO "RPT01" (
  "统一社会信用代码","单位详细名称","数据来源","是否一套表单位"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  "数据来源",
  "是否一套表单位"
FROM "temp611";


--- 2. 更新资产信息字段信息 ---
UPDATE RPT01
SET "经营地 - 省（自治区、直辖市）" = t."省（自治区、直辖市）",
  "经营地 - 市（地、州、盟）" = t."市（地、州、盟）",
  "经营地 - 县（区、市、旗）" = t."县（区、市、旗）",
  "经营地 - 乡（镇、街道办事处）" = t."乡（镇、街道办事处）",
  "是否与单位所在地详细地址一致" = t."是否与单位所在地详细地址一致：",
  "注册地 - 省（自治区、直辖市）" = t."省（自治区、直辖市）注册地",
  "注册地 - 市（地、州、盟）" = t."市（地、州、盟）注册地",
  "注册地 - 县（区、市、旗）" = t."县（区、市、旗）注册地",
  "注册地 - 乡（镇、街道办事处）" = t."乡（镇、街道办事处）注册地",
  "机构类型（代码）"=t."机构类型",
  "登记注册类别（代码）"=t."登记注册统计类别",
  "行业代码"=t."行业代码",
  "单位规模（代码）"=t."单位规模",
  "主要业务活动"=t."主要业务活动1"
FROM "611" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=false;


UPDATE RPT01
SET "经营地 - 省（自治区、直辖市）" = t."省（自治区、直辖市）",
  "经营地 - 市（地、州、盟）" = t."地（市、州、盟）",
  "经营地 - 县（区、市、旗）" = t."县（市、区、旗）",
  "经营地 - 乡（镇、街道办事处）" = t."乡（镇、街道）",
  "是否与单位所在地详细地址一致" = t."是否与单位所在地详细地址一致",
  "注册地 - 省（自治区、直辖市）" = t."省（自治区、直辖市）注册地区",
  "注册地 - 市（地、州、盟）" = t."地（市、州、盟）注册地区",
  "注册地 - 县（区、市、旗）" = t."县（市、区、旗）注册地区",
  "注册地 - 乡（镇、街道办事处）" = t."乡（镇、街道）注册地区",
  "机构类型（代码）"=t."机构类型",
  "登记注册类别（代码）"=t."登记注册统计类别",
  "行业代码"=t."行业代码",
  "单位规模（代码）"=t."单位规模",
  "主要业务活动"=t."主要业务活动1"
FROM "601" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=true;

UPDATE RPT01
  SET "机构类型（文字）" = CASE "机构类型（代码）"
    WHEN '10' THEN '企业'
    WHEN '20' THEN '事业单位'
    WHEN '30' THEN '机关'
    WHEN '40' THEN '社会团体'
    WHEN '51' THEN '民办非企业单位'
    WHEN '52' THEN '基金会'
    WHEN '53' THEN '居委会'
    WHEN '54' THEN '村委会'
    WHEN '55' THEN '农民专业合作社'
    WHEN '56' THEN '农村集体经济组织'
    WHEN '90' THEN '其他组织机构'
    ELSE ''
  END;

UPDATE RPT01
  SET "单位规模（文字）" = CASE "单位规模（代码）"
    WHEN '1' THEN '大型'
    WHEN '2' THEN '中型'
    WHEN '3' THEN '小型'
    WHEN '4' THEN '微型'
    ELSE ''
  END;

UPDATE RPT01
SET "登记注册类别（文字）" = CASE "登记注册类别（代码）"
  -- 内资企业
  WHEN '111' THEN '国有独资公司'
  WHEN '112' THEN '私营有限责任公司'
  WHEN '119' THEN '其他有限责任公司'
  WHEN '121' THEN '私营股份有限公司'
  WHEN '129' THEN '其他股份有限公司'
  WHEN '131' THEN '全民所有制企业(国有企业)'
  WHEN '132' THEN '集体所有制企业(集体企业)'
  WHEN '133' THEN '股份合作企业'
  WHEN '134' THEN '联营企业'
  WHEN '140' THEN '个人独资企业'
  WHEN '150' THEN '合伙企业'
  WHEN '190' THEN '其他内资企业'
  -- 港澳台投资企业
  WHEN '210' THEN '港澳台投资有限责任公司'
  WHEN '220' THEN '港澳台投资股份有限公司'
  WHEN '230' THEN '港澳台投资合伙企业'
  WHEN '290' THEN '其他港澳台投资企业'
  -- 外商投资企业
  WHEN '310' THEN '外商投资有限责任公司'
  WHEN '320' THEN '外商投资股份有限公司'
  WHEN '330' THEN '外商投资合伙企业'
  WHEN '390' THEN '其他外商投资企业'
  -- 其他市场主体
  WHEN '400' THEN '农民专业合作社（联合社）'
  WHEN '500' THEN '个体工商户'
  WHEN '900' THEN '其他市场主体'
  ELSE ''
END;


UPDATE RPT01
SET "产业" = ic."产业分类","行业门类" = ic."匹配专用门类","行业中类" = ic."中类（三维）","行业大类" = ic."大类（2位）",  "专业分类" = ic."专业分类", "行业小类" = ic."小类（四位）"
FROM "ICNEA-1" AS ic
WHERE RPT01."行业代码" = ic."行业代码2";

--- 3. 更新指标信息字段信息 ---
--- 3.1 B603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."其中：本年折旧211;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失（损失以—号记）;本年"*0.1,
    "信用减值损失" = t."信用减值损失（损失以—号记）;本年"*0.1,
    "投资收益" = t."投资收益（损失以—号记）;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益（损失以—号记）;本年"*0.1,
    "资产处置收益" = t."资产处置收益（损失以—号记）;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益（损失以—号记）;本年"*0.1
FROM "B603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='B603';

--- 3.2 C603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."其中：本年折旧1;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失（损失以—号记）;本年"*0.1,
    "信用减值损失" = t."信用减值损失（损失以—号记）;本年"*0.1,
    "投资收益" = t."投资收益损失以—号记;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益损失以—号记;本年"*0.1,
    "资产处置收益" = t."资产处置收益损失以—号记;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益损失以—号记;本年"*0.1
FROM "C603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='C603';


--- 3.3 E603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."其中：本年折旧211;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失（损失以—号记）;本年"*0.1,
    "信用减值损失" = t."信用减值损失（损失以—号记）;本年"*0.1,
    "投资收益" = t."投资收益（损失以—号记）;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益（损失以—号记）;本年"*0.1,
    "资产处置收益" = t."资产处置收益（损失以—号记）;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益（损失以—号记）;本年"*0.1
FROM "E603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='E603';


--- 3.4 F603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."其中：本年折旧211;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失损失以—号记;本年"*0.1,
    "信用减值损失" = t."信用减值损失损失以—号记;本年"*0.1,
    "投资收益" = t."投资收益损失以—号记;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益损失以—号记;本年"*0.1,
    "资产处置收益" = t."资产处置收益损失以—号记;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益损失以—号记;本年"*0.1
FROM "F603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='F603';


--- 3.5 S603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."其中：本年折旧211;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失（损失以—号记）;本年"*0.1,
    "信用减值损失" = t."信用减值损失（损失以—号记）;本年"*0.1,
    "投资收益" = t."投资收益（损失以—号记）;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益（损失以—号记）;本年"*0.1,
    "资产处置收益" = t."资产处置收益（损失以—号记）;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益（损失以—号记）;本年"*0.1
FROM "S603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='S603';


--- 3.6 X603信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
"本年折旧" = t."固定资产累计折旧-其中：本年折旧;本年"*0.1,
"资产总计" = t."资产总计;本年"*0.1,"负债合计" = t."负债合计;本年"*0.1,
"营业收入" = t."营业收入;本年"*0.1,"税金及附加" = t."税金及附加;本年"*0.1,
"应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
"应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
"营业利润" = t."营业利润;本年"*0.1,
"资产减值损失" = t."资产减值损失（损失以—号记）;本年"*0.1,
"信用减值损失" = t."信用减值损失（损失以—号记）;本年"*0.1,
"投资收益" = t."投资收益（损失以—号记）;本年"*0.1,
"公允价值变动收益" = t."公允价值变动收益（损失以—号记）;本年"*0.1,
"资产处置收益" = t."资产处置收益（损失以—号记）;本年"*0.1,
"其他收益" = t."其他收益;本年"*0.1,
"净敞口套期收益" = t."净敞口套期收益（损失以—号记）;本年"*0.1
FROM "X603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='X603';


--- 3.7 611-3信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本;本年"*0.1,
    "本年折旧" = t."本年折旧;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "投资收益" = t."投资收益;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "研发人员数" = t."研究开发人员合计;本年"*0.1,
    "研发费用" = t."研究开发费用合计;本年"*0.1,
FROM "611-3" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-3';


--- 3.8 611-4信息字段信息 ---

UPDATE RPT01
SET "营业成本" = t."营业成本（营业支出）;本年"*0.1,
    "本年折旧" = t."其中：本年折旧;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."营业收入;本年"*0.1,
    "税金及附加" = t."税金及附加;本年"*0.1,
    "应交增值税" = t."应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."应付职工薪酬（本年贷方累计发生额）;本年"*0.1,
    "营业利润" = t."营业利润;本年"*0.1,
    "资产减值损失" = t."资产减值损失（损失以－号记）;本年"*0.1,
    "信用减值损失" = t."信用减值损失（损失以－号记）;本年"*0.1,
    "投资收益" = t."投资收益（损失以－号记）;本年"*0.1,
    "公允价值变动收益" = t."公允价值变动收益（损失以－号记）;本年"*0.1,
    "资产处置收益" = t."资产处置收益（损失以－号记）;本年"*0.1,
    "其他收益" = t."其他收益;本年"*0.1,
    "净敞口套期收益" = t."净敞口套期收益（损失以－号记）;本年"*0.1,
    "研发费用" = t."研发费用;本年"*0.1
FROM "611-4" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-4';


--- 3.10 611-6信息字段信息 ---

UPDATE RPT01
SET "本年折旧" = t."其中：本年折旧2;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "营业收入" = t."经营收入;本年"*0.1,
    "税金及附加" = t."税金及附加费用;本年"*0.1,
    "应交增值税" = t."四、应交增值税（本年累计发生额）;本年"*0.1,
    "应付职工薪酬" = t."其中：工资福利支出;本年"*0.1
FROM "611-6" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-6';


--- 3.11 611-7信息字段信息 ---

UPDATE RPT01
SET "本年折旧" = t."其中：本年折旧;本年"*0.1,
    "资产总计" = t."资产总计;本年"*0.1,
    "负债合计" = t."负债合计;本年"*0.1,
    "应交增值税" = 
    COALESCE(t."税费3;本年", 0)*0.1
    + COALESCE(t."税费;本年", 0)*0.1,
    "应付职工薪酬" = 
    COALESCE(t."其中：人员费用3;本年", 0)*0.1
    + COALESCE(t."其中：人员费用;本年", 0)*0.1,
    "增加值" =
    COALESCE(t."其中：人员费用3;本年", 0)*0.1
  + COALESCE(t."其中：人员费用;本年", 0)*0.1
  + COALESCE(t."税费3;本年", 0)*0.1
  + COALESCE(t."税费;本年", 0)*0.1
  + COALESCE(t."固定资产折旧3;本年", 0)*0.1
  + COALESCE(t."固定资产折旧;本年", 0)*0.1
FROM "611-7" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-7';

--- 3.11 工业字段信息 ---

UPDATE RPT01
SET "工业总产值"=i."工业总产值;本年"*0.1
FROM "B604-1" AS i
WHERE RPT01."统一社会信用代码" = i."统一社会信用代码";


--- 4. 更新额外信息字段信息 ---

UPDATE RPT01
SET "期末从业人数" = t."从业人员期末人数;本年",
    "其中女性从业人数" = t."其中：女性;本年"
FROM "602" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=true;

update RPT01
SET "期末从业人数" = t."从业人员期末人数",
    "其中女性从业人数" = t."其中：女性"
FROM "601" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=true;

UPDATE RPT01
SET "期末从业人数" = t."从业人员期末人数;本年",
    "其中女性从业人数" = t."其中：女性;本年"
FROM "611-2" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=false;


UPDATE RPT01
SET "研发人员数" = t."研究开发人员合计;本年",
    "研发费用" = t."研究开发费用合计;本年"*0.1,
    "专利申请数" = t."当年专利申请数;本年",
    "专利授权数" = t."期末有效发明专利数;本年"
FROM "607-2" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."是否一套表单位"=true;


--- 5. 合计计算字段信息 ---

UPDATE RPT01
SET "税收合计" = 
    COALESCE("应交增值税", 0) 
  + COALESCE("税金及附加", 0);

--- 3.1 B603信息字段信息 ---

UPDATE RPT01
SET "增加值" =
    COALESCE("本年折旧", 0)
  + COALESCE("税金及附加", 0)
  + COALESCE("应交增值税", 0)
  + COALESCE(t."上交政府的各项非税费用;本年", 0)*0.1
  + COALESCE(t."其中：上缴的各项税费;本年", 0)*0.1
  + COALESCE("应付职工薪酬", 0)
  + COALESCE(t."其他属于劳动者报酬的部分;本年", 0)*0.1
  + COALESCE(t."董事会费;本年", 0)*0.532*0.1
  + COALESCE(t."差旅费;本年", 0)*0.064*0.1
  - COALESCE(t."职工教育经费;本年", 0)*0.1
  + COALESCE("营业利润", 0)
  - COALESCE("资产减值损失", 0)
  - COALESCE("信用减值损失", 0)
  - COALESCE("投资收益", 0)
  - COALESCE("公允价值变动收益", 0)
  - COALESCE("资产处置收益", 0)
  - COALESCE("其他收益", 0)
  - COALESCE("净敞口套期收益", 0)
  + COALESCE(t."其中：上交管理费;本年", 0)*0.1
  + COALESCE(t."其中：利息费用;本年", 0)*0.06*0.1
  - COALESCE(t."利息收入;本年", 0)*0.06*0.1
FROM "B603" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='B603';

UPDATE RPT01
SET "增加值" =
    COALESCE("本年折旧", 0)
  + COALESCE("税金及附加", 0)
  + COALESCE("应交增值税", 0)
  + COALESCE("应付职工薪酬", 0)
  + COALESCE("营业利润", 0)
  - COALESCE("资产减值损失", 0)
  - COALESCE("信用减值损失", 0)
  - COALESCE("投资收益", 0)
  - COALESCE("公允价值变动收益", 0)
  - COALESCE("资产处置收益", 0)
  - COALESCE("其他收益", 0)
  - COALESCE("净敞口套期收益", 0)
WHERE "数据来源" NOT IN ('B603') AND "是否一套表单位"=true;


UPDATE RPT01
SET "增加值" =
    COALESCE("本年折旧", 0)
  + COALESCE("税金及附加", 0)
  + COALESCE("应交增值税", 0)
  + COALESCE("应付职工薪酬", 0)
  + COALESCE("营业利润", 0)
  - COALESCE("投资收益", 0)
  - COALESCE("其他收益", 0)
FROM "611-3" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-3';


--- 3.8 611-4信息字段信息 ---

UPDATE RPT01
SET "增加值" =
    COALESCE("本年折旧", 0)
  + COALESCE("税金及附加", 0)
  + COALESCE("应交增值税", 0)
  + COALESCE("应付职工薪酬", 0)
  + COALESCE("营业利润", 0)
  - COALESCE("资产减值损失", 0)
  - COALESCE("信用减值损失", 0)
  - COALESCE("投资收益", 0)
  - COALESCE("公允价值变动收益", 0)
  - COALESCE("资产处置收益", 0)
  - COALESCE("其他收益", 0)
  - COALESCE("净敞口套期收益", 0)
FROM "611-4" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-4';


--- 3.10 611-6信息字段信息 ---

UPDATE RPT01
SET "增加值" =
    COALESCE("应付职工薪酬", 0)
  + COALESCE(t."其中：劳务费;本年", 0)*0.1
  + COALESCE(t."福利费;本年", 0)*0.1
  + COALESCE(t."对个人和家庭的补助;本年", 0)*0.5*0.1
  + COALESCE(t."工会经费;本年", 0)*0.1
  + COALESCE("税金及附加", 0)
  + COALESCE("应交增值税", 0)
  + COALESCE("本年折旧", 0)
  + COALESCE("营业收入", 0)
  - COALESCE(t."经营支出;本年", 0)*0.1,
FROM "611-6" AS t
WHERE RPT01."统一社会信用代码" = t."统一社会信用代码" AND RPT01."数据来源"='611-6';

UPDATE RPT01
SET "单位规模（代码）" = CASE
-- 农、林、牧、渔业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '农、林、牧、渔专业及辅助性活动' AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "营业收入" >=20000 THEN '1'
            WHEN "营业收入" >=500 AND "营业收入" <20000 THEN '2'
            WHEN "营业收入" >=50 AND "营业收入" <500 THEN '3'
            WHEN "营业收入" <50 THEN '4'
        END
-- 工业（从业人数 + 营业收入）
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '工业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=1000 AND "营业收入" >=40000 THEN '1'
            WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=2000 AND "营业收入" <40000 THEN '2'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <300 AND "营业收入" >=300 AND "营业收入" <2000 THEN '3'
            WHEN "期末从业人数" <20 AND "营业收入" <300 THEN '4'
        END
-- 建筑业（营业收入 + 资产总计）
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '建筑业' AND "营业收入" IS NOT NULL AND "资产总计" IS NOT NULL THEN
        CASE
            WHEN "营业收入" >=80000 AND "资产总计" >=80000 THEN '1'
            WHEN "营业收入" >=6000 AND "营业收入" <80000 AND "资产总计" >=5000 AND "资产总计" <80000 THEN '2'
            WHEN "营业收入" >=300 AND "营业收入" <6000 AND "资产总计" >=300 AND "资产总计" <5000 THEN '3'
            WHEN "营业收入" <300 AND "资产总计" <300 THEN '4'
        END
-- 继续补充完整内容：用户输入内容过长，拆分为多段写入
-- 批发业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '批发业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=200 AND "营业收入" >=40000 THEN '1'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <200 AND "营业收入" >=5000 AND "营业收入" <40000 THEN '2'
            WHEN "期末从业人数" >=5 AND "期末从业人数" <20 AND "营业收入" >=1000 AND "营业收入" <5000 THEN '3'
            WHEN "期末从业人数" <5 AND "营业收入" <1000 THEN '4'
        END
-- 零售业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '零售业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 AND "营业收入" >=20000 THEN '1'
            WHEN "期末从业人数" >=50 AND "期末从业人数" <300 AND "营业收入" >=500 AND "营业收入" <20000 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <50 AND "营业收入" >=100 AND "营业收入" <500 THEN '3'
            WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
        END
-- 邮政业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '邮政业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=1000 AND "营业收入" >=30000 THEN '1'
            WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=2000 AND "营业收入" <30000 THEN '2'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <300 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
            WHEN "期末从业人数" <20 AND "营业收入" <100 THEN '4'
        END
-- 住宿业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '住宿业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 AND "营业收入" >=10000 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=2000 AND "营业收入" <10000 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
            WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
        END
-- 餐饮业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '餐饮业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 AND "营业收入" >=10000 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=2000 AND "营业收入" <10000 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
            WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
        END
-- 批发业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '批发业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=200 AND "营业收入" >=40000 THEN '1'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <200 AND "营业收入" >=5000 AND "营业收入" <40000 THEN '2'
            WHEN "期末从业人数" >=5 AND "期末从业人数" <20 AND "营业收入" >=1000 AND "营业收入" <5000 THEN '3'
            WHEN "期末从业人数" <5 AND "营业收入" <1000 THEN '4'
        END
-- 零售业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '零售业' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 AND "营业收入" >=20000 THEN '1'
            WHEN "期末从业人数" >=50 AND "期末从业人数" <300 AND "营业收入" >=500 AND "营业收入" <20000 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <50 AND "营业收入" >=100 AND "营业收入" <500 THEN '3'
            WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
        END
-- 交通运输业（多个行业大类组合）
    WHEN "单位规模（代码）" IS NULL AND "行业大类" IN ('道路运输业', '水上运输业', '航空运输业', '管道运输业', '多式联运和运输代理业') AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=1000 AND "营业收入" >=30000 THEN '1'
            WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=3000 AND "营业收入" <30000 THEN '2'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <300 AND "营业收入" >=200 AND "营业收入" <3000 THEN '3'
            WHEN "期末从业人数" <20 AND "营业收入" <200 THEN '4'
        END
-- 装卸搬运
    WHEN "单位规模（代码）" IS NULL AND "行业中类" = '装卸搬运' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=1000 AND "营业收入" >=30000 THEN '1'
            WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=3000 AND "营业收入" <30000 THEN '2'
            WHEN "期末从业人数" >=20 AND "期末从业人数" <300 AND "营业收入" >=200 AND "营业收入" <3000 THEN '3'
            WHEN "期末从业人数" <20 AND "营业收入" <200 THEN '4'
        END

-- 仓储业
WHEN "单位规模（代码）" IS NULL AND "行业中类" IN ('通用仓储', '低温仓储', '危险品仓储', '谷物、棉花等农产品仓储', '中药材仓储', '其他仓储业')
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=200 AND "营业收入" >=30000 THEN '1'
        WHEN "期末从业人数" >=100 AND "期末从业人数" <200 AND "营业收入" >=1000 AND "营业收入" <30000 THEN '2'
        WHEN "期末从业人数" >=20 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <1000 THEN '3'
        WHEN "期末从业人数" <20 AND "营业收入" <100 THEN '4'
    END

-- 住宿业
WHEN "单位规模（代码）" IS NULL AND "行业门类" = '住宿业'
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=300 AND "营业收入" >=10000 THEN '1'
        WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=2000 AND "营业收入" <10000 THEN '2'
        WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
        WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
    END

-- 餐饮业
WHEN "单位规模（代码）" IS NULL AND "行业门类" = '餐饮业'
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=300 AND "营业收入" >=10000 THEN '1'
        WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=2000 AND "营业收入" <10000 THEN '2'
        WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
        WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
    END

-- 邮政业
WHEN "单位规模（代码）" IS NULL AND "行业门类" = '邮政业'
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=1000 AND "营业收入" >=30000 THEN '1'
        WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=2000 AND "营业收入" <30000 THEN '2'
        WHEN "期末从业人数" >=20 AND "期末从业人数" <300 AND "营业收入" >=100 AND "营业收入" <2000 THEN '3'
        WHEN "期末从业人数" <20 AND "营业收入" <100 THEN '4'
    END

-- 信息传输业
WHEN "单位规模（代码）" IS NULL AND "行业门类" = '信息传输业'
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=2000 AND "营业收入" >=100000 THEN '1'
        WHEN "期末从业人数" >=100 AND "期末从业人数" <2000 AND "营业收入" >=1000 AND "营业收入" <100000 THEN '2'
        WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=100 AND "营业收入" <1000 THEN '3'
        WHEN "期末从业人数" <10 AND "营业收入" <100 THEN '4'
    END

-- 软件和信息技术服务业
WHEN "单位规模（代码）" IS NULL AND "行业大类" = '软件和信息技术服务业'
    AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
    CASE
        WHEN "期末从业人数" >=300 AND "营业收入" >=10000 THEN '1'
        WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=1000 AND "营业收入" <10000 THEN '2'
        WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "营业收入" >=50 AND "营业收入" <1000 THEN '3'
        WHEN "期末从业人数" <10 AND "营业收入" <50 THEN '4'
    END

-- 房地产业
WHEN "单位规模（代码）" IS NULL AND "行业中类" = '房地产开发经营'
    AND "营业收入" IS NOT NULL AND "资产总计" IS NOT NULL THEN
    CASE
        WHEN "营业收入" >=200000 AND "资产总计" >=10000 THEN '1'
        WHEN "营业收入" >=1000 AND "营业收入" <200000 AND "资产总计" >=5000 AND "资产总计" <10000 THEN '2'
        WHEN "营业收入" >=100 AND "营业收入" <1000 AND "资产总计" >=2000 AND "资产总计" <5000 THEN '3'
        WHEN "营业收入" <100 AND "资产总计" <2000 THEN '4'
    END


-- 物业管理
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '物业管理' AND "期末从业人数" IS NOT NULL AND "营业收入" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=1000 AND "营业收入" >=5000 THEN '1'
            WHEN "期末从业人数" >=300 AND "期末从业人数" <1000 AND "营业收入" >=1000 AND "营业收入" <5000 THEN '2'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "营业收入" >=500 AND "营业收入" <1000 THEN '3'
            WHEN "期末从业人数" <100 AND "营业收入" <500 THEN '4'
        END

-- 租赁和商务服务业
    WHEN "单位规模（代码）" IS NULL AND "行业门类" = '租赁和商务服务业' AND "期末从业人数" IS NOT NULL AND "资产总计" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 AND "资产总计" >=120000 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 AND "资产总计" >=8000 AND "资产总计" <120000 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 AND "资产总计" >=100 AND "资产总计" <8000 THEN '3'
            WHEN "期末从业人数" <10 AND "资产总计" <100 THEN '4'
        END

-- 科学研究和技术服务业、水利、环境和公共设施管理业等
    WHEN "单位规模（代码）" IS NULL AND "行业门类" IN (
        '科学研究和技术服务业',
        '水利、环境和公共设施管理业',
        '居民服务、修理和其他服务业',
        '文化、体育和娱乐业',
        ''
    ) AND "期末从业人数" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 THEN '3'
            WHEN "期末从业人数" <10 THEN '4'
        END

-- 社会工作
    WHEN "单位规模（代码）" IS NULL AND "行业大类" = '社会工作' AND "期末从业人数" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 THEN '3'
            WHEN "期末从业人数" <10 THEN '4'
        END

-- 房地产中介、其他房地产业
    WHEN "单位规模（代码）" IS NULL AND "行业中类" IN ('房地产中介服务', '其他房地产业') AND "期末从业人数" IS NOT NULL THEN
        CASE
            WHEN "期末从业人数" >=300 THEN '1'
            WHEN "期末从业人数" >=100 AND "期末从业人数" <300 THEN '2'
            WHEN "期末从业人数" >=10 AND "期末从业人数" <100 THEN '3'
            WHEN "期末从业人数" <10 THEN '4'
        END

-- 兜底：保留原值
    ELSE "单位规模（代码）"
END;


-- 步骤 2：映射单位规模（文字）
UPDATE RPT01
SET "单位规模（文字）" = CASE "单位规模（代码）"
    WHEN '1' THEN '大型'
    WHEN '2' THEN '中型'
    WHEN '3' THEN '小型'
    WHEN '4' THEN '微型'
    ELSE ''
END;


DELETE FROM "RPT01"
WHERE "产业" = '第一产业';


INSERT INTO "RPT02" (
  "统一社会信用代码",
  "单位详细名称",
  "经营地 - 省（自治区、直辖市）",
  "经营地 - 市（地、州、盟）",
  "经营地 - 县（区、市、旗）",
  "经营地 - 乡（镇、街道办事处）",
  "是否与单位所在地详细地址一致",
  "注册地 - 省（自治区、直辖市）",
  "注册地 - 市（地、州、盟）",
  "注册地 - 县（区、市、旗）",
  "注册地 - 乡（镇、街道办事处）",
  "机构类型（代码）",
  "行业代码",
  "登记注册类别（代码）",
  "法人单位详细名称",
  "法人单位详细地址",
  "法人单位区划代码",
  "从业人员期末人数",
  "从业人员期末人数其中女性",
  "经营性单位收入",
  "非经营性单位支出 （费用）"
)
SELECT
  "统一社会信用代码",
  "单位详细名称",
  "省（自治区、直辖市）" AS "经营地 - 省（自治区、直辖市）",
  "市（地、州、盟）",
  "县（区、市、旗）",
  "乡（镇、街道办事处）",
  "是否与单位所在地详细地址一致：",
  "省（自治区、直辖市）注册地",
  "市（地、州、盟）注册地",
  "县（区、市、旗）注册地",
  "乡（镇、街道办事处）注册地",
  "机构类型",
  "行业代码",
  "登记注册统计类别",
  "法人单位详细名称",
  "法人单位详细地址",
  "法人单位区划代码",
  "从业人员期末人数",
  "从业人员期末人数其中女性",
  "经营性单位收入"*0.1,
  "非经营性单位支出 （费用）"*0.1

FROM "611" WHERE "单位类型（最新）"= '2';

UPDATE RPT02
  SET "机构类型（文字）" = CASE "机构类型（代码）"
    WHEN '10' THEN '企业'
    WHEN '20' THEN '事业单位'
    WHEN '30' THEN '机关'
    WHEN '40' THEN '社会团体'
    WHEN '51' THEN '民办非企业单位'
    WHEN '52' THEN '基金会'
    WHEN '53' THEN '居委会'
    WHEN '54' THEN '村委会'
    WHEN '55' THEN '农民专业合作社'
    WHEN '56' THEN '农村集体经济组织'
    WHEN '90' THEN '其他组织机构'
    ELSE ''
  END;

UPDATE RPT02
SET "登记注册类别（文字）" = CASE "登记注册类别（代码）"
  -- 内资企业
  WHEN '111' THEN '国有独资公司'
  WHEN '112' THEN '私营有限责任公司'
  WHEN '119' THEN '其他有限责任公司'
  WHEN '121' THEN '私营股份有限公司'
  WHEN '129' THEN '其他股份有限公司'
  WHEN '131' THEN '全民所有制企业(国有企业)'
  WHEN '132' THEN '集体所有制企业(集体企业)'
  WHEN '133' THEN '股份合作企业'
  WHEN '134' THEN '联营企业'
  WHEN '140' THEN '个人独资企业'
  WHEN '150' THEN '合伙企业'
  WHEN '190' THEN '其他内资企业'
  -- 港澳台投资企业
  WHEN '210' THEN '港澳台投资有限责任公司'
  WHEN '220' THEN '港澳台投资股份有限公司'
  WHEN '230' THEN '港澳台投资合伙企业'
  WHEN '290' THEN '其他港澳台投资企业'
  -- 外商投资企业
  WHEN '310' THEN '外商投资有限责任公司'
  WHEN '320' THEN '外商投资股份有限公司'
  WHEN '330' THEN '外商投资合伙企业'
  WHEN '390' THEN '其他外商投资企业'
  -- 其他市场主体
  WHEN '400' THEN '农民专业合作社（联合社）'
  WHEN '500' THEN '个体工商户'
  WHEN '900' THEN '其他市场主体'
  ELSE ''
END;


UPDATE RPT02
SET "产业" = ic."产业分类","行业门类" = ic."匹配专用门类","行业中类" = ic."中类（三维）","行业大类" = ic."大类（2位）", "行业小类" = ic."小类（四位）"
FROM "ICNEA-1" AS ic
WHERE RPT02."行业代码" = ic."行业代码2";


INSERT INTO "RPT03" (
  "统一社会信用代码",
  "单位详细名称",
  "数据来源",
  "是否一套表单位",
  "经营地 - 省（自治区、直辖市）",
  "经营地 - 市（地、州、盟）",
  "经营地 - 县（区、市、旗）",
  "经营地 - 乡（镇、街道办事处）",
  "是否与单位所在地详细地址一致",
  "注册地 - 省（自治区、直辖市）",
  "注册地 - 市（地、州、盟）",
  "注册地 - 县（区、市、旗）",
  "注册地 - 乡（镇、街道办事处）",
  "机构类型（代码）",
  "机构类型（文字）",
  "登记注册类别（代码）",
  "登记注册类别（文字）",
  "行业代码",
  "产业",
  "行业门类",
  "行业大类",
  "行业中类",
  "行业小类",
  "营业成本",
  "本年折旧",
  "资产总计",
  "负债合计",
  "营业收入",
  "税金及附加",
  "应交增值税",
  "应付职工薪酬",
  "营业利润",
  "资产减值损失",
  "信用减值损失",
  "投资收益",
  "公允价值变动收益",
  "资产处置收益",
  "其他收益",
  "净敞口套期收益",
  "税收合计",
  "期末从业人数",
  "其中女性从业人数",
  "研发人员数",
  "研发费用",
  "专利申请数",
  "专利授权数",
  "增加值",
  "单位规模（代码）",
  "单位规模（文字）",
  "产业分类",
  "产业领域",
  "产业大类",
  "产业中类",
  "产业小类"
)
SELECT
  r."统一社会信用代码",
  r."单位详细名称",
  r."数据来源",
  r."是否一套表单位",
  r."经营地 - 省（自治区、直辖市）",
  r."经营地 - 市（地、州、盟）",
  r."经营地 - 县（区、市、旗）",
  r."经营地 - 乡（镇、街道办事处）",
  r."是否与单位所在地详细地址一致",
  r."注册地 - 省（自治区、直辖市）",
  r."注册地 - 市（地、州、盟）",
  r."注册地 - 县（区、市、旗）",
  r."注册地 - 乡（镇、街道办事处）",
  r."机构类型（代码）",
  r."机构类型（文字）",
  r."登记注册类别（代码）",
  r."登记注册类别（文字）",
  r."行业代码",
  r."产业",
  r."行业门类",
  r."行业大类",
  r."行业中类",
  r."行业小类",
  r."营业成本",
  r."本年折旧",
  r."资产总计",
  r."负债合计",
  r."营业收入",
  r."税金及附加",
  r."应交增值税",
  r."应付职工薪酬",
  r."营业利润",
  r."资产减值损失",
  r."信用减值损失",
  r."投资收益",
  r."公允价值变动收益",
  r."资产处置收益",
  r."其他收益",
  r."净敞口套期收益",
  r."税收合计",
  r."期末从业人数",
  r."其中女性从业人数",
  r."研发人员数",
  r."研发费用",
  r."专利申请数",
  r."专利授权数",
  r."增加值",
  r."单位规模（代码）",
  r."单位规模（文字）",
  n."统计分类",
  n."领域",
  n."大类名称",
  n."中类名称",
  n."小类名称"
FROM "RPT01" AS r
INNER JOIN "NTRISC" AS n
  ON r."行业代码" = n."国民经济行业分类代码";


INSERT INTO "RPT04" (
  "统一社会信用代码",
  "单位详细名称",
  "经营地 - 省（自治区、直辖市）",
  "经营地 - 市（地、州、盟）",
  "经营地 - 县（区、市、旗）",
  "经营地 - 乡（镇、街道办事处）",
  "是否与单位所在地详细地址一致",
  "注册地 - 省（自治区、直辖市）",
  "注册地 - 市（地、州、盟）",
  "注册地 - 县（区、市、旗）",
  "注册地 - 乡（镇、街道办事处）",
  "机构类型（代码）",
  "机构类型（文字）",
  "登记注册类别（代码）",
  "登记注册类别（文字）",
  "行业代码",
  "产业",
  "行业门类",
  "行业大类",
  "行业中类",
  "行业小类",
  "法人单位详细名称",
  "法人单位详细地址",
  "法人单位区划代码",
  "从业人员期末人数",
  "从业人员期末人数其中女性",
  "经营性单位收入",
  "非经营性单位支出 （费用）",
  "产业分类",
  "产业领域",
  "产业大类",
  "产业中类",
  "产业小类"
)
SELECT
  r."统一社会信用代码",
  r."单位详细名称",
  r."经营地 - 省（自治区、直辖市）",
  r."经营地 - 市（地、州、盟）",
  r."经营地 - 县（区、市、旗）",
  r."经营地 - 乡（镇、街道办事处）",
  r."是否与单位所在地详细地址一致",
  r."注册地 - 省（自治区、直辖市）",
  r."注册地 - 市（地、州、盟）",
  r."注册地 - 县（区、市、旗）",
  r."注册地 - 乡（镇、街道办事处）",
  r."机构类型（代码）",
  r."机构类型（文字）",
  r."登记注册类别（代码）",
  r."登记注册类别（文字）",
  r."行业代码",
  r."产业",
  r."行业门类",
  r."行业大类",
  r."行业中类",
  r."行业小类",
  r."法人单位详细名称",
  r."法人单位详细地址",
  r."法人单位区划代码",
  r."从业人员期末人数",
  r."从业人员期末人数其中女性",
  r."经营性单位收入",
  r."非经营性单位支出 （费用）",
  n."统计分类",
  n."领域",
  n."大类名称",
  n."中类名称",
  n."小类名称"
FROM "RPT02" AS r
INNER JOIN "NTRISC" AS n
  ON r."行业代码" = n."国民经济行业分类代码";
