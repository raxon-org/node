{{$request = request()}}
{{$options = options()}}
{{$class = data.extract('options.class')}}
{{$force = data.extract('options.force')}}
{{if(is.empty($class))}}
You need to provide the option class for the new class name.
{{else}}
{{Raxon.Node:Data:object.create(
$class,
Raxon.Node:Role:role_system(),
$options,
[
'force' => $force
])}}
{{/if}}