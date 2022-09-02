###
### NOTE: `get_from_redshift` is accessed through running the magic command:
###      %run /Users/denis.lapchev@vgw.co/Utilities/redshift_helper_prod

sql = """select game_name,
       count(distinct date(created_date)) as days,
       sum(case when win_count>1 then 1 else 0 end) as feature_win,
       sum(case when win_amount>bet_amount then 1 else 0 end) as big_win,
       sum(case when win_amount>0 then 1 else 0 end) as hit,
       sum(case when win_count>1 and win_amount>0 then 1 else 0 end) as feature_win_not_zero,
       count(1) as spins,
       sqrt(variance(win_amount/bet_amount)) as sd,
       sqrt(variance(case when win_amount>0 then win_amount/bet_amount end)) as sd_wins,
       sqrt(variance(case when win_count>1 then win_amount/bet_amount end) ) as sd_features,
       avg(win_amount/bet_amount) as mean_rtp,
       avg(case when win_amount>0 then win_amount/bet_amount end) as mean_win_rtp,
       avg(case when win_count>1 then win_amount/bet_amount end) as mean_features_rtp
       
from chumba_casino.bets_wins_each_rgs_lag
WHERE
created_date between '2020-10-01' and '2023-05-31' -- after 31 of Jan 2022 no RGS game have win_count>1, i.e. features aren't adequately recorded anymore? 
and coin_type_id = 4
and (jackpot is null OR jackpot is false)
GROUP BY 1
HAVING spins > 10000
UNION
SELECT game_name,
       count(distinct date(created_date)) as days,
       sum(case when win_count>1 then 1 else 0 end) as feature_win,
       sum(case when win_amount>bet_amount then 1 else 0 end) as big_win,
       sum(case when win_amount>0 then 1 else 0 end) as hit,
       sum(case when win_count>1 and win_amount>0 then 1 else 0 end) as feature_win_not_zero,
       count(1) as spins,
       sqrt(variance(win_amount/bet_amount)) as sd,
       sqrt(variance(case when win_amount>0 then win_amount/bet_amount end)) as sd_wins,
       sqrt(variance(case when win_count>1 then win_amount/bet_amount end) ) as sd_features,
       avg(win_amount/bet_amount) as mean_rtp,
       avg(case when win_amount>0 then win_amount/bet_amount end) as mean_win_rtp,
       avg(case when win_count>1 then win_amount/bet_amount end) as mean_features_rtp
      
FROM chumba_casino.bets_wins_each_chu_lag
WHERE
created_date between '2020-10-01' and '2023-05-31'
and (jackpot is null OR jackpot is false)
and coin_type_id = 4
GROUP BY 1
HAVING spins > 10000
"""

data,cols = get_from_redshift(sql, return_column_names=True)
df_feat_win = pd.DataFrame(data,columns=cols).round(2)
df_feat_win['combined_feature_frequency'] = round(df_feat_win['spins']/df_feat_win['feature_win'])
df_feat_win['hit_rate'] = round(df_feat_win['spins'] / df_feat_win['hit'])
volatilities = df_feat_win[['game_name', 'sd']].set_index('game_name')
