<?php

namespace Raxon\Org\Node\Trait\Data;

use Raxon\Org\App;
use Raxon\Org\Config;

use Raxon\Org\Module\Controller;
use Raxon\Org\Module\Core;
use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\Dir;
use Raxon\Org\Module\File;

use Raxon\Org\Node\Service\Security;

use Exception;

trait Rename {

    /**
     * @throws Exception
     */
    public function rename($from, $to, $role, $options=[]): bool
    {
        $object = $this->object();
        $from = Controller::name($from);
        $to = Controller::name($to);
        $options = Core::object($options, Core::OBJECT_ARRAY);
        if(!array_key_exists('function', $options)){
            $options['function'] = __FUNCTION__;
        }
        if(!Security::is_granted(
            $from,
            $role,
            $options
        )){
            return false;
        }
        $dir_object = $object->config('project.dir.node') .
            'Object' .
            $object->config('ds')
        ;
        $url_data_from = $object->config('project.dir.node') . 'Data' . $object->config('ds') . $from . $object->config('extension.json');
        $url_data_to = $object->config('project.dir.node') . 'Data' . $object->config('ds') . $to . $object->config('extension.json');
        $url_expose_from = $object->config('project.dir.node') . 'Expose' . $object->config('ds') . $from . $object->config('extension.json');
        $url_expose_to = $object->config('project.dir.node') . 'Expose' . $object->config('ds') . $to . $object->config('extension.json');
        $url_object_from = $object->config('project.dir.node') . 'Object' . $object->config('ds') . $from . $object->config('extension.json');
        $url_object_to = $object->config('project.dir.node') . 'Object' . $object->config('ds') . $to . $object->config('extension.json');
        $url_validate_from = $object->config('project.dir.node') . 'Validate' . $object->config('ds') . $from . $object->config('extension.json');
        $url_validate_to = $object->config('project.dir.node') . 'Validate' . $object->config('ds') . $to . $object->config('extension.json');
        // 4 options: -force (overwrite file) or -skip (skip) or -merge (merge patch) or -merge-overwite (merge overwrite)
        $force = false;
        if(array_key_exists('force', $options)){
            $force = $options['force'];
        }
        $merge = false;
        if(array_key_exists('merge', $options)){
            $merge = $options['merge'];
        }
        $skip = false;
        if(array_key_exists('skip', $options)){
            $skip = $options['skip'];
        }
        $merge_overwrite = false;
        if(array_key_exists('merge-overwrite', $options)){
            $merge_overwrite = $options['merge-overwrite'];
        }
        if(
            File::exist($url_data_to) &&
            $force === false &&
            $merge === false &&
            $skip === false &&
            $merge_overwrite === false
        ){
            throw new Exception('To (Data) ('. $to .') already exists');
        }
        if(
            File::exist($url_expose_to) &&
            $force === false &&
            $merge === false &&
            $skip === false &&
            $merge_overwrite === false
        ){
            throw new Exception('To (Expose) ('. $to .') already exists');
        }
        if(
            File::exist($url_object_to) &&
            $force === false &&
            $merge === false &&
            $skip === false &&
            $merge_overwrite === false
        ){
            throw new Exception('To (Object) ('. $to .') already exists');
        }
        if(
            File::exist($url_validate_to) &&
            $force === false &&
            $merge === false &&
            $skip === false &&
            $merge_overwrite === false
        ){
            throw new Exception('To (Validate) ('. $to .') already exists');
        }
        $list = [];
        if(File::exist($url_data_from)){
            $merger = new Storage();
            $read = $object->data_read($url_data_from);
            if(
                (
                    $merge ||
                    $skip ||
                    $merge_overwrite
                ) &&
                File::exist($url_data_to)
            ){
                $write_to = $object->data_read($url_data_to);
                if($write_to){
                    foreach($write_to->data($to) as $record){
                        if(
                            is_array($record) &&
                            array_key_exists('uuid', $record)
                        ){
                            $merger->set($record['uuid'], $record);
                        }
                        elseif(
                            is_object($record) &&
                            property_exists($record, 'uuid')
                        ){
                            $merger->set($record->uuid, $record);
                        }
                    }
                }
                $write = new Storage();
            }
            elseif($force &&
                File::exist($url_data_to)
            ){
                File::delete($url_data_to);
                $write = new Storage();
            } else {
                $write = new Storage();
            }
            if($read){
                foreach($read->data($from) as $record){
                    if(
                        is_array($record) &&
                        array_key_exists('uuid', $record)
                    ){
                        $record['#class'] = $to;
                        if(
                            $skip &&
                            $merger->has($record['uuid'])
                        ){
                            //merge skip
                            continue;
                        }
                        elseif(
                            $merge_overwrite &&
                            $merger->has($record['uuid'])
                        ){
                            //merge overwrite
                            //use of $record
                        }
                        elseif(
                            $merge &&
                            $merger->has($record['uuid'])
                        ){
                            //merge patch
                            $record = array_merge($merger->get($record['uuid']), $record);
                        }
                        $merger->delete($record['uuid']);
                    }
                    elseif(
                        is_object($record) &&
                        property_exists($record, 'uuid')
                    ) {
                        $record->{'#class'} = $to;
                        if(
                            $skip &&
                            $merger->has($record->uuid)
                        ){
                            //merge skip
                            continue;
                        }
                        elseif(
                            $merge_overwrite &&
                            $merger->has($record->uuid)
                        ){
                            //merge overwrite
                            //use of $record
                        }
                        elseif(
                            $merge &&
                            $merger->has($record->uuid)
                        ){
                            //merge patch
                            $record = Core::object_merge($merger->get($record->uuid), $record);
                        }
                        $merger->delete($record->uuid);
                    }
                    $list[] = $record;
                }
            }
            foreach($merger->data() as $record){
                $list[] = $record;
            }
            $write->set($to, $list);
            $url_data_write = $write->write($url_data_to);
        } else {
            //can still process expose, object & validate & relations
        }
        if(!File::exist($url_expose_from)){
            throw new Exception('From (Expose) ('. $from .') does not exist');
        }
        $read = $object->data_read($url_expose_from);
        $write = new Storage();
        if($read){
            $write->data($to, $read->data($from));
        }
        if(Core::object_is_empty($write->data())){
            throw new Exception('Empty expose write, fix from: ' . $url_expose_from);
        }
        if(File::exist($url_expose_to)){
            File::delete($url_expose_to);
        }
        $url_expose_write =$write->write($url_expose_to);
        if(!File::exist($url_object_from)){
            throw new Exception('From (Object) ('. $from .') does not exist');
        }
        $read = $object->data_read($url_object_from);
        $write = new Storage();
        if($read){
            $write->data($read->data());
        }
        if(Core::object_is_empty($write->data())){
            throw new Exception('Empty object write, fix from: ' . $url_object_from);
        }
        if(File::exist($url_object_to)){
            File::delete($url_object_to);
        }
        $url_object_write = $write->write($url_object_to);
        if(!File::exist($url_validate_from)){
            throw new Exception('From (Validate) ('. $from .') does not exist');
        }
        $read = $object->data_read($url_validate_from);
        $write = new Storage();
        if($read){
            $write->data($to, $read->data($from));
        }
        if(Core::object_is_empty($write->data($to))){
            throw new Exception('Empty validate write, fix from: ' . $url_object_from);
        }
        if(File::exist($url_validate_to)){
            File::delete($url_validate_to);
        }
        $url_validate_write = $write->write($url_validate_to);
        if(
            $url_expose_write &&
            $url_object_write &&
            $url_validate_write
        ){
            $dir = new Dir();
            $read = $dir->read($dir_object);
            if(
                $read &&
                is_array($read)
            ){
                foreach($read as $file){
                    if($file->type === Dir::TYPE){
                        continue;
                    }
                    $read_data = $object->data_read($file->url);
                    if($read_data){
                        $relations = $read_data->get('relation');
                        if(empty($relations)){
                            continue;
                        }
                        foreach($relations as $nr => $relation){
                            if(
                                property_exists($relation, 'class') &&
                                $relation->class === $from
                            ){
                                $relation->class = $to;
                                $relations[$nr] = $relation;
                            }
                        }
                        $read_data->set('relation', $relations);
                        $read_data->write($file->url);
                    }
                }
            }
            if($from !== $to){
                File::delete($url_data_from);
                File::delete($url_expose_from);
                File::delete($url_object_from);
                File::delete($url_validate_from);
            }
            return true;
        }
        return false;
    }
}