<@extends src="base.ftl">

<@block name="content">

<dl><h2>Queues</h2>
<#list This.queues as queue>
 <span class="listing">
   <dt class="item"><a href="${This.path}/${queue.name.schemeSpecificPart}">${queue.name.schemeSpecificPart}</a></dt>to summarize<dd></dd>
</#list>
</dl>

</@block>

</@extends>