<meta charset="UTF-8">
<meta http-equiv="Content-type" content="text/html; charset=UTF-8">

<h3>Object Retrieval Error</h3>
<p>Error has occurred during retrieval of object: ${object}</p>

<#if unreachableStorages?has_content>
<p>Storages which are not reachable: <#list unreachableStorages as us>${us}<#sep>, </#list></p>
</#if>

<#if invalidStorages?has_content>
<p>Storages which failed because of invalid checksum: <#list invalidStorages as is>${is}<#sep>, </#list></p>
</#if>

<#if errorStorages?has_content>
<p>Storages which failed for other reason: <#list errorStorages as es>${es}<#sep>, </#list></p>
</#if>

<#if successfulStorage?has_content>
<p>Storage which successfully retrieved object: ${successfulStorage}</p>
</#if>

<#if recoveredStorages?has_content>
<p>Storages which was updated with the valid copy of object: <#list recoveredStorages as rs>${rs}<#sep>, </#list></p>
</#if>

<p>${conclusion}</p>

<p>
    ${appName}, ${appUrl}
</p>

<p>This e-mail was generated automatically. Please do not respond.</p>