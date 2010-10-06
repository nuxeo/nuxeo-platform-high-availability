<@extends src="base.ftl">

<#assign info=This.info >

<@block name="content">
<h2>Queue info</h2>
<p class="info">
${info.name}
was last handled at  <span class="was last handled">${info.lastHandlingTime?datetime}</span>
                and <span class="is in state">is now ${info.state}</span>.
</p>
</@block>

<@block name="toolbox">
<ul>
 <h3>Toolbox</h3>
<#if info.orphaned || info.failed>
 <li><a href="${This.path}/@retry">Retry</a></li>
 <li><a href="${This.path}/@blacklist">Blacklist</a></li>
</#if>
<#if info.blacklisted>
 <li><a href="${This.path}/@purge">Purge</a></li>
</#if>
</ul>
</@block>

</@extends>