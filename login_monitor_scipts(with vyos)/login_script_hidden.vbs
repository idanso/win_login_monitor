command = "powershell.exe -nologo -command C:\login_monitor_scipts\login_script.ps1 --force"
 set shell = CreateObject("WScript.Shell")
 shell.Run command,0