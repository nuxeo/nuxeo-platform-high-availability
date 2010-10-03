<@extends src="base.ftl">


<@block name="content">

<dl><h2>${This.manager.name.schemeSpecificPart} queue</h2>
<#list This.infos as info>
 <span class="listing">
      <dt><a href="${This.name}/${info.name.fragment}">${info.name}</a></dt>
       <dd>was last handled at  <span class="was last handled">${info.lastHandlingTime?datetime}</span>
                and <span class="is in state">is now ${info.state}</span>.  </dd>
</#list>
</dl>

</@block>

<@block name="toolbox">
<ul><h3>Toolbox</h3>
<li><a href="${This.path}/@blacklist">Blacklist</a></li>
<li><a href="${This.path}/@retry">Retry</a></li>
<li><a href="${This.path}/@purge">Purge</a></li>
</ul>
</@block>

</@extends>