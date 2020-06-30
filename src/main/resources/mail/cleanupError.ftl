<meta charset="UTF-8">
<meta http-equiv="Content-type" content="text/html; charset=UTF-8">

<h3>Archival Storage Cleanup Error</h3>
<p>Total number of objects affected by cleanup: ${total} </p>

<#if deleted?has_content>
<p>Successfully deleted ${deleted?size} objects: <#list deleted as d>${d}<#sep>, </#list></p>
</#if>

<#if rolledBack?has_content>
<p>Successfully rolled back ${rolledBack?size} objects: <#list rolledBack as r>${r}<#sep>, </#list></p>
</#if>

<#if failed?has_content>
<p>Failed to delete/rollback ${failed?size} objects: <#list failed as f>${f}<#sep>, </#list></p>
</#if>

<p>
    ${appName}, ${appUrl}
</p>

<p>This e-mail was generated automatically. Please do not respond.</p>