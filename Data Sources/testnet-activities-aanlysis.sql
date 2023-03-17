WITH cohort_users AS (
  SELECT
      MIN(DATE(signed_at)) as cohort_date,
      tx_sender,
      chain_name 
    FROM
      blockchains.all_chains
    WHERE
      chain_name in ('arbitrum_testnet','bsc_testnet','matic_mumbai', 'base_testnet')
    GROUP BY tx_sender, chain_name
    ORDER BY cohort_date, chain_name ASC
),

new_users AS (
  SELECT
    *,
  SUM(new_users) OVER(PARTITION BY chain_name ORDER BY date) as users_accumulated
  FROM
    (SELECT
        cohort_date as date,
        chain_name,
        uniq(tx_sender) as new_users,
        row_number() OVER(PARTITION BY chain_name ORDER BY date) as row_id
    FROM
      cohort_users
    GROUP BY date,chain_name
    ORDER BY row_id, chain_name
    ) AS subquery
  WHERE row_id <= 40
),

all_users as (
  SELECT
    *,
    SUM(transactions) OVER (PARTITION BY chain_name ORDER BY date) as tx_accumulated,
    SUM(total_gas_spent) OVER (PARTITION BY chain_name ORDER BY date) as gas_accumulated,
    lagInFrame(active_users) OVER (PARTITION BY chain_name ORDER BY date) as previous_users
  FROM   
    (SELECT
      DATE(signed_at) as date,
      chain_name,
      row_number() OVER (PARTITION BY chain_name ORDER BY date) as row_id,
      uniq(tx_hash) as transactions,
      uniq(tx_sender) as active_users,
      AVG(tx_gas_spent) as avg_gas_spent,
      SUM(tx_gas_spent) as total_gas_spent,
      AVG(tx_gas_price/pow(10,9)) as avg_gas_price
      
    FROM
      blockchains.all_chains
    WHERE
      chain_name in ('arbitrum_testnet','bsc_testnet','matic_mumbai', 'base_testnet')
    GROUP BY date, chain_name
    ORDER BY chain_name, row_id ASC) AS subquery
  WHERE row_id <= 40
)

SELECT
  a.row_id,
  a.date,
  a.chain_name,
  transactions,
  tx_accumulated,
  active_users,
  new_users,
  users_accumulated,
  (new_users/active_users) as new_users_percent,
  (CASE
    WHEN COALESCE(((active_users - new_users)/previous_users),0) > 1 THEN 1 ELSE COALESCE(((active_users - new_users)/previous_users),0)
  END) AS retention_ratio,
  avg_gas_spent,
  total_gas_spent,
  gas_accumulated,
  avg_gas_price
FROM
  all_users a
  LEFT JOIN new_users n ON a.date= n.date AND n.chain_name = a.chain_name
WHERE transactions <> 0
  AND active_users > 2
ORDER BY a.chain_name, a.row_id


/*
SELECT
    DATE(signed_at) as date,
    uniq(tx_hash) as transactions,
    uniq(tx_sender) as active_users,
    AVG(tx_gas_spent) as avg_gas_spent,
    SUM(tx_gas_spent) as total_gas_spent,
    AVG(tx_gas_price) as avg_gas_price
    
FROM
    blockchains.all_chains
WHERE
    chain_name  = 'base_testnet'
GROUP BY date
ORDER BY date
*/



  