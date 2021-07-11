$uid = 'usr'
$pass = 'password'
$db = 'db'
$dev_db = 'dev_db'
#getting the default gateway (vyos) ip address
$DGAddress = $(ipconfig | where {$_ -match 'Default Gateway . . . . . . . . . :+\s(1\d{1,2}\.\d{1,3}.\d{1,3}.\d{1,3})' } | out-null; $Matches[1])

#ssh to vyos for getting the RDP ip address of the rdp
$passwd = ConvertTo-SecureString -String 'vyos' -AsPlainText -Force 
$UserName = 'vyos'
$creds = New-Object System.Management.Automation.PSCredential($UserName, $passwd)
$session = New-SSHSession -ComputerName $DGAddress -Credential $creds -Force
$stream = $session.Session.CreateShellStream("dumb", 0, 0, 0, 0, 1000)
Start-Sleep -Seconds 1
$output = $stream.Read()
$stream.Write("show interfaces`n`n")
Start-Sleep -Seconds 1
$output = $stream.Read()
$header = "10.24"
$tmp = ($output -split $header,2)[1]
$ip_address = '10.24' + $tmp.Substring(0,9)

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
        $sql.CommandText = "CALL insert_to_login_out_by_ip('$ip_address', 'Logins')"
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
            $sql.CommandText = "CALL insert_to_login_out_by_ip('$ip_address', 'Logins')"
            $sql.ExecuteNonQuery()
        }
    }
    $Connection.Close()
}