<meta charset="UTF-8">
<meta http-equiv="Content-type" content="text/html; charset=UTF-8">

<h3>Some objects are in error state or processing for too long</h3>
<p>
It is recommended to investigate the problem and use cleanup feature to clean the storage.
</p>

<p>
Objects (databaseId, storageId, creation, state):
<br>
<#list objects as o>${o}<#sep>
<br>
</#list>
</p>

<p>
    ${appName}, ${appUrl}
</p>

<p>This e-mail was generated automatically. Please do not respond.</p>