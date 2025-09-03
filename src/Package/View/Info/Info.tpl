{{$request = request()}}
Package: {{$request.package}}

Module: {{$request.module|>string.uppercase.first}}

{{if(!is.empty($request.submodule))}}
Submodule: {{$request.submodule|>string.uppercase.first}}
{{/if}}
{{if($request.module === 'info')}}
{{$files = dir.read(config('controller.dir.view') + 'Object/')}}
{{$files = data.sort($files, ['url' => 'ASC'])}}
Commands:
{{foreach($files as $file)}}
{{$file.basename = file.basename($file.name, config('extension.tpl'))}}
{{dd($file.basename)}}
{{binary()}} {{$request.package}} object {{$file.basename|>string.lowercase}}

{{/foreach}}
{{else}}
{{$options = options()}}
{{$is.all = false}}
{{if(is.empty.object($options))}}
{{$is.all = true}}
{{$files = dir.read(config('controller.dir.view') + 'Object/Info/')}}
{{$files = data.sort($files, ['url' => 'ASC'])}}
Options:
{{foreach($files as $file)}}
{{if($file.name === 'Object.Info.tpl')}}
{{continue()}}
{{/if}}
{{$file.basename = file.basename($file.name, config('extension.tpl'))}}
{{if(!is.empty($options[$file.basename|>string.lowercase]) || !is.empty($is.all))}}
{{binary()}} {{$request.package}} {{$request.module}} {{$request.submodule|>default:''}} -{{$file.basename|>string.lowercase}}
{{/if}}
{{/foreach}}
{{else}}
{{$files = dir.read(config('controller.dir.view') + 'Object/Info/')}}
{{$files = data.sort($files, ['url' => 'ASC'])}}
{{foreach($files as $file)}}
{{if($file.name === 'Object.Info.tpl')}}
{{continue()}}
{{/if}}
{{$file.basename = file.basename($file.name, config('extension.tpl'))}}
{{if(!is.empty($options[$file.basename|>string.lowercase]) || !is.empty($is.all))}}
{{require($file.url)}}
{{/if}}
{{/foreach}}
{{/if}}
{{/if}}