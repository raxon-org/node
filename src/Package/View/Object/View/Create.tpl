{{R3M}}
{{$request = request()}}
{{$options = options()}}
Package: {{$request.package}}

Module: {{$request.module|uppercase.first}}

Submodule: {{$request.submodule|uppercase.first}}

{{$class = data.extract('options.class')}}
{{if(is.empty($class))}}
You need to provide the option class.
Available classes:
{{$read = dir.read(config('project.dir.node') + 'Object/')}}
{{if(is.array($read))}}
{{$read = data.sort($read, ['name' => 'ASC'])}}
{{for.each($read as $file)}}
- {{file.basename($file.name, config('extension.json'))}}

{{/for.each}}
{{/if}}
{{else}}
{{$response = Raxon.Org.Node:Data:view.create(
$class,
Raxon.Org.Node:Role:role.system(),
$options
)}}
{{$response|json.encode:'JSON_PRETTY_PRINT'}}

{{/if}}