WITH china AS
         (SELECT DISTINCT a.short_code as china_id
          FROM spaceman_public.accounts a
                   LEFT JOIN spaceman_public.reservations r
                             ON r.account_uuid = a.uuid
                   LEFT JOIN spaceman_public.locations l
                             ON r.location_id = l.id
          WHERE l.country_code = 'CHN'
         ),

     amex AS (
         SELECT a.short_code as amex_codes
         FROM dw.mv_fact_global_access_reservation gr
                  LEFT JOIN spaceman_public.accounts a
                            ON gr.account_id = a.id
         WHERE gr.notes ilike '%amex%'
     ),

     user_emails AS (
         SELECT DISTINCT email, ua.user_id
         FROM spaceman_public.users u
                  LEFT JOIN spaceman_public.user_accounts as ua
                            on ua.user_id = u.id and ua.role IN ('["admin"]', '["billing"]', '["billing","member"]')
                                AND ua.status = 'active'
     ),

sent_mails AS (
     SELECT em._id,
           ac.short_code                                                                as account_short_code,
           date(date_trunc('month', to_date(mail_time, 'yyyy-mm-dd hh:mi:ss'))) as mail_date_month,
           mail_time,
           NVL(email_tag, template)                                             as email_type,
           opens,
           case when opens >= 1 THEN em._id ELSE NULL END                       as opened_mails,
           COUNT(DISTINCT CASE
                              WHEN split_part(opens_detail_ua, '/', 1) = 'Mobile' THEN em._id
                              ELSE NULL END)                                    as unique_mails_opened_mobile,
           SUM(CASE
                   WHEN split_part(opens_detail_ua, '/', 1) = 'Mobile' THEN 1
                   ELSE 0 END)                                                     mobile_opens,
           clicks,
           case when clicks >= 1 THEN em._id ELSE NULL END                      as clicked_mails,
           SUM(CASE
                   WHEN split_part(clicks_detail_ua, '/', 1) = 'Mobile' THEN 1
                   ELSE 0 END)                                                     mobile_clicks,
           COUNT(DISTINCT CASE
                              WHEN split_part(clicks_detail_ua, '/', 1) = 'Mobile'
                                  THEN em._id
                              ELSE NULL END)                                    as unique_mails_clicked_mobile,
           COUNT(DISTINCT CASE
                              WHEN clicks_detail_url IN ('http://facebook.com/wework', 'http://instagram.com/wework',
                                                         'https://www.linkedin.com/company/wework',
                                                         'http://twitter.com/wework')
                                  THEN NULL
                              WHEN clicks_detail_url IS NULL THEN NULL
                              WHEN clicks_detail_url = '' THEN NULL

                              ELSE _id END)                                     as unique_relevant_clicked_mails,
           SUM(CASE
                   WHEN clicks_detail_url IN ('http://facebook.com/wework', 'http://instagram.com/wework',
                                              'https://www.linkedin.com/company/wework',
                                              'http://twitter.com/wework')
                       THEN 0
                   WHEN clicks_detail_url IS NULL THEN 0
                   WHEN clicks_detail_url = '' THEN 0
                   ELSE 1 END)                                                  as num_relevant_clicks
FROM (SELECT DISTINCT _id,
                          email,
                          mail_time,
                          state,
                          subject,
                          email_tag,
                          sender,
                          template,
                          opens,
                          opens_detail_location,
                          opens_detail_ts,
                          opens_detail_ua,
                          opens_detail_ip,
                          clicks,
                          clicks_detail_location,
                          clicks_detail_ts,
                          clicks_detail_url,
                          clicks_detail_ua,
                          clicks_detail_ip,
                          account_shortcode,
                          location_code,
                          resends,
                          reject,
                          bounce_description,
                          bgtools_code,
                          subaccount,
                          diag,
                          insert_time
          FROM email_data_model.dwh_mandrill_data
          WHERE (email_tag in ('anywhere-upcoming-notification',
                               'payments-adyen-dd-attempt-directdebit_gb',
                               'payments-adyen-dd-attempt-sepadirectdebit',
                               'payments-attempt-ach',
                               'payments-chargeback-adyen-dd',
                               'payments-declined-ach',
                               'payments-declined-cc',
                               'refund_bank_info_form',
                               'upcoming_bill_notification',
                               'anywhere-upcoming-notification'
              )
              OR template ilike '%late-payment%'
              OR template ilike '%commons-monthly-payment-failed%'
              OR template ilike '%no-payment-info%'
              OR template ilike '%overage-warning%'
              OR template ilike '%invoice-finalized-notice-with-pdf-attached-en%'
              OR template ilike '%enterprise-invoice-finalized-notice-with-pdf%'
              )
            AND state = 'sent'
            AND to_date(mail_time, 'yyyy-mm-dd HH:MI:SS') >= '2019-04-01'
            AND to_date(mail_time, 'yyyy-mm-dd HH:MI:SS') < '2019-07-01'
          ) em

LEFT JOIN user_emails
    on user_emails.email = em.email
LEFT JOIN spaceman_public.accounts as ac on (ac.short_code = em.account_shortcode) OR (em.account_shortcode IS NULL AND user_emails.user_id = ac.account_admin_id)
WHERE short_code NOT IN (SELECT china_id FROM china)
AND short_code NOT IN (SELECT amex.amex_codes FROM amex)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 10, 11)


SELECT mail_date_month,
       email_type,
       COUNT(DISTINCT account_short_code)         as num_accounts,
       count(distinct _id)                as sent_mails,
       sum(opens)                         as total_opens,
       COUNT(DISTINCT opened_mails)       as opened_mails,
       SUM(unique_mails_opened_mobile)    as unique_mobile_opened_mails,
       SUM(mobile_opens)                  as mobile_opens,
       sum(clicks)                        as total_clicks,
       COUNT(DISTINCT clicked_mails)      as clicked_mails,
       SUM(unique_mails_clicked_mobile)   as unique_mobile_clicked_mails,
       SUM(mobile_clicks)                 as mobile_clicks,
       SUM(unique_relevant_clicked_mails) as unique_relevant_clicks,
       SUM(num_relevant_clicks)           as relevant_clicks
FROM sent_mails
GROUP BY 1, 2;



