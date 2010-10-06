<@extends src="base.ftl">

<@block name="content">

<dl><h2>Queues</h2>
<#list This.queues as queue>
 <span class="listing">
   <dt class="item"><a href="${This.path}/${queue.name.schemeSpecificPart}">${queue.name.schemeSpecificPart}</a></dt><dd><span class="is handling ">is handling </span> 
   <span class="handling count">${queue.listHandledContent()?size}</span> items 
   <#if queue.listFailedContent()?size gt 0> and has ${queue.listFailedContent()?size} failed content</#if>
   <#if queue.listOrphanedContent()?size gt 0 > and has ${queue.listOrphanedContent()?size} orphaned content</#if>.</dd>
</#list>
</dl>

</@block>

</@extends>