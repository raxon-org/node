{{translation.import()}}
{{$request = request()}}
{{$selected = (int) parameter($request.package, 1)}}
{{$list = parse.read(config('controller.dir.data') + 'Command.json', true, (object) ['array_fast' => true])}}
{{$sort = Sort::list($list.command)}}
{{$list.command = $sort->with(['command' => 'asc'])}}
Package: {{$request.package|>string.lowercase|>string.uppercase.first}}

{{if(!is.empty($request.module))}}Module: {{$request.module|>string.lowercase|>string.uppercase.first}}

{{/if}}
{{if(!is.empty($list.command))}}
{{$nr = 1}}{{if($selected > 0)}}{{else}}Commands:
{{/if}}{{foreach($list.command as $item)}}
{{$key = $nr}}
{{if($key < 10)}}
{{$key = '0' + $key}}
{{/if}}{{if($selected > 0)}}{{if($selected === $nr)}}
{{$execute = string.trim.right($item.command + ' ' + implode(' ', flags('#command')) + ' ' + implode(' ', options('#command')), ' ')}}
Executing ({{$execute}})...
{{terminal.interactive()}}
{{execute($execute)}}
{{/if}}{{else}}[{{$key}}] {{$item.command}}

{{/if}}{{$nr++}}
{{/foreach}}
{{$nr = 1}}{{if($selected > 0)}}{{else}}
Description:
{{/if}}{{foreach($list.command as $item)}}
{{$key = $nr}}
{{if($key < 10)}}
{{$key = '0' + $key}}
{{/if}}{{if($selected > 0)}}{{else}}[{{$key}}] {{implode(PHP_EOL, $item.description)}}

{{/if}}{{$nr++}}
{{/foreach}}
{{/if}}