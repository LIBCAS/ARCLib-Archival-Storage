<meta charset="UTF-8">
<meta http-equiv="Content-type" content="text/html; charset=UTF-8">

<h3>Initial Storages Check Warning</h3>
<#if storagesCount < minStorages>
<p>There are not enough logical storages attached. Minimum defined by configuration: ${minStorages} but only ${storagesCount} attached.</p>
</#if>
<#if unreachableServices?has_content>
<p>Following storages are not reachable: <#list unreachableServices as s>${s}<#sep>, </#list></p>
</#if>

<p>
    ${appName}, ${appUrl}
</p>

<p>This e-mail was generated automatically. Please do not respond.</p>