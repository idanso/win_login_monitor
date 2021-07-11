command = "powershell.exe -nologo -command C:\login_monitor_scipts\logout_script.ps1 --force"
 set shell = CreateObject("WScript.Shell")
 shell.Run command,0