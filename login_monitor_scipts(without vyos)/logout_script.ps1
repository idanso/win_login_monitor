$uid = 'usr'
$pass = 'password'
$db = 'db'
$dev_db = 'dev_db'
# get computer ip
$ip_address = $(ipconfig | where {$_ -match 'IPv4.+\s(10.24\d{1}\.\d{1,3}\.\d{1,3})' } | out-null; $Matches[1])


if($ip_address.StartsWith('10.10'))
{
    $db_host = 'db_host'
}
elseif($ip_address.StartsWith('10.11'))
{
    $db_host = 'db_host2'
}
else
{
    $db_host = $null
}

if($db_host -ne $null)
{
    #connect the library MySql.Data.dll
    Add-Type –Path ‘C:\Program Files (x86)\MySQL\MySQL Connector Net 8.0.25\Assemblies\v4.5.2\MySql.Data.dll'

    # database connection
    $Connection = [MySql.Data.MySqlClient.MySqlConnection]@{ConnectionString="server=$db_host;uid=$uid;pwd=$pass;database=$db;SslMode=None"}
    $Connection.Open()
    $sql = New-Object MySql.Data.MySqlClient.MySqlCommand
    $sql.Connection = $Connection

    $sql.CommandText = "CALL select_ip_proc('$ip_address');"
    $response = $sql.ExecuteReader()
    if ($response.Read())
    {
        $Connection.Close()
        $Connection = [MySql.Data.MySqlClient.MySqlConnection]@{ConnectionString="server=$db_host;uid=$uid;pwd=$pass;database=$db;SslMode=None"}
        $Connection.Open()
        $sql = New-Object MySql.Data.MySqlClient.MySqlCommand
        $sql.Connection = $Connection
        #call DB procedure
        $sql.CommandText = "CALL insert_to_login_out_by_ip('$ip_address', 'Logouts')"
        $sql.ExecuteReader()
    }

    else
    {
        $Connection.Close()
        $Connection = [MySql.Data.MySqlClient.MySqlConnection]@{ConnectionString="server=$db_host;uid=$uid;pwd=$pass;database=$dev_db;SslMode=None"}
        $Connection.Open()
        $sql = New-Object MySql.Data.MySqlClient.MySqlCommand
        $sql.Connection = $Connection
        $sql.CommandText = "CALL select_ip_proc('$ip_address');"
        $response = $sql.ExecuteReader()
        if ($response.Read())
        {
            $Connection.Close()
            $Connection = [MySql.Data.MySqlClient.MySqlConnection]@{ConnectionString="server=$db_host;uid=$uid;pwd=$pass;database=$dev_db;SslMode=None"}
            $Connection.Open()
            $sql = New-Object MySql.Data.MySqlClient.MySqlCommand
            $sql.Connection = $Connection
            #call DB procedure
            $sql.CommandText = "CALL insert_to_login_out_by_ip('$ip_address', 'Logouts')"
            $sql.ExecuteNonQuery()
        }
    }
    $Connection.Close()
}