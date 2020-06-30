<meta charset="UTF-8">
<meta http-equiv="Content-type" content="text/html; charset=UTF-8">

<h3>Inconsistent objects data/metadata found at storage ${storageId}</h3>

<#if inconsistentObjects?has_content>
<p>
Objects which content were not consistent:
<br>
<#list inconsistentObjects as io>${io}<#sep>
<br>
</#list>
</p>
<p>
Objects which were successfully recovered from other storage:
<br>
<#list recoveredObjects as ro>${ro}<#sep>
<br>
</#list>
</p>
</#if>

<#if inconsistentMetadata?has_content>
<p>
Objects which metadata were not consistent:
<br>
<#list inconsistentMetadata as im>${im}<#sep>
<br>
</#list>
</p>
<p>
Objects which metadata were successfully recovered from other storage:
<br>
<#list recoveredMetadata as rm>${rm}<#sep>
<br>
</#list>
</p>
</#if>

<p><b>${conclusion}</b></p>

<p>
    ${appName}, ${appUrl}
</p>

<p>This e-mail was generated automatically. Please do not respond.</p>