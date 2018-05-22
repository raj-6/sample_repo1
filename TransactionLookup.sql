SELECT a.userid, u.UserName, t.AccountNumber, CashoutAmount/100, tip, a.CreatedOn, RestoreDate
FROM Activations a
join transactions t
on t.ActivationID = a.ActivationID
join users u
on u.userid = a.UserID
where --AccountNumber = '021214860000865'
    FirstName = 'Amanda' and LastName = 'Lassa'
and Date in ('2017-11-10') -- Date after what the bank lists (file v posted)
and TransactionTypeID in (3,7)
and t.StatusID = 2
and t.Amount != Tip
order by 3
/*
50  2.00    2017-08-22 16:16:00.260
30  0.00    2017-08-20 16:38:09.983
30  0.00    2017-08-22 18:49:08.163
50  0.00    2017-08-17 16:45:39.507
*/
SELECT * FROM LoginDetails WHERE USERID = 1403629 AND TmStamp <= '2017-10-29' ORDER BY 1 DESC
/*
50  2.00    2017-08-22 16:15:30.384 iPhone8,2
30  0.00    2017-08-20 16:37:40.043 iPhone8,2
30  0.00    2017-08-22 18:48:49.616 iPhone8,2
50  0.00    2017-08-17 16:44:23.499 iPhone8,2
*/
SELECT * FROM Devices WHERE UDID = 'FED835F2-1102-46DB-97F6-EF0E93D168E4'
/*!!!!!!!!!!!Look in client state database for IP address, SSID and Carrier!!!!!!!!!!!!*/


-- Edited version of the above to only have to run one query
SELECT a.userid, u.UserName, identifiers.AccountNumber, a.CashoutAmount/100 CashoutAmount,
    a.tip, a.CreatedOn, a.RestoreDate, ld.UDID, ld.TmStamp
FROM (
    SELECT a.ActivationId, t.AccountNumber, max(ld.LoginID) LoginId
    FROM Activations a (nolock)
    join transactions t (nolock)
    on t.ActivationID = a.ActivationID
    join users u (nolock)
    on u.userid = a.UserID
    inner join LoginDetails ld (nolock)
    on ld.UserId = a.UserId and ld.TmStamp <= a.CreatedOn
    where --AccountNumber = '021214860000865'
        FirstName = 'Amanda' and LastName = 'Lassa'
    and Date in ('2017-11-10') -- Date after what the bank lists (file v posted)
    and TransactionTypeID in (3,7)
    and t.StatusID = 2
    and t.Amount != Tip
    AND ld.IsSuccessfulLogin = 1
    group by a.ActivationID, t.AccountNumber
) identifiers
inner join Activations a (nolock)
on identifiers.ActivationId = a.ActivationId
inner join LoginDetails ld (nolock)
on identifiers.LoginId = ld.LoginID
inner join users u (nolock)
on u.userid = a.userid
order by 3



/*
 * SWITCH DATABASES HERE!
 *
 */
 use clientstate;
 SELECT l.LocationId, n.carriername, n.IpAddress, n.NetworkId, n.NetworkType, n.Status, n.TimeStamp
 FROM clientstate.Locations l
 LEFT JOIN clientstate.ClientState cs
   ON l.LocationId = cs.LocationId
 LEFT JOIN clientstate.Networks n
   ON cs.NetworkId = n.ClientStateNetworkId
 WHERE l.UserId = 1228108
 AND l.TimeStamp > '2017-09-14' AND l.TimeStamp < '2017-09-17'
