<?php

namespace Raxon\Node\Trait;

use Raxon\App;
use Raxon\Config;

use Raxon\Module\Cli;
use Raxon\Module\Controller;
use Raxon\Module\Core;
use Raxon\Module\Data as Storage;
use Raxon\Module\File;
use Raxon\Module\Parse;

use Exception;

use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;
use Raxon\Exception\AuthorizationException;

trait Expose {

    /**
     * @throws AuthorizationException
     * @throws ObjectException
     * @throws Exception
     */
    public function expose($node, $expose=[], $class='', $function='', $internalRole=false, $parentRole=false): Storage
    {
        $list = [$node];
        $list = $this->expose_list($list, $expose, $class, $function, $internalRole, $parentRole);
        if(
            is_array($list) &&
            array_key_exists(0, $list)
        ){
            return $list[0];
        }
        return new Storage();
    }

    /**
     * @throws ObjectException
     * @throws Exception
     * @throws AuthorizationException
     */
    public function expose_list($nodeList, $expose=[], $class='', $function='', $internalRole=false, $parentRole=false): mixed
    {
        $object = $this->object();
        if (!is_array($expose)) {
            return [];
        }
        $roles = [];
        if ($internalRole) {
            $roles[] = $internalRole; //same as parent
        } else {
//            $roles = Permission::getAccessControl($object, $class, $function);
            try {
                /*
                $user = User::getByAuthorization($object);
                if ($user) {
                    $roles = $user->getRolesByRank('asc');
                }
                */
            } catch (Exception $exception) {

            }
        }
        if (empty($roles)) {
            throw new Exception('Roles failed...');
        }
        $is_expose = false;
        foreach ($roles as $role) {
            if (
                property_exists($role, 'name') &&
                property_exists($role, 'permission') &&
                is_array($role->permission)
            ) {

                foreach ($role->permission as $permission) {
                    foreach ($expose as $action) {
                        if (
                            (
                                property_exists($permission, 'name') &&
                                $permission->name === str_replace('.', ':', Controller::name($class)) . ':' . str_replace('_', '.', $function) &&
                                property_exists($action, 'role') &&
                                $action->role === $role->name
                            )
                            ||
                            (
                                in_array(
                                    $function,
                                    ['child', 'children'],
                                    true
                                ) &&
                                property_exists($action, 'role') &&
                                $action->role === $parentRole
                            )
                        ) {
                            $is_expose = true;

                            foreach($nodeList as $nr => $node) {
                                $record = [];
                                if (
                                    property_exists($action, 'property') &&
                                    is_array($action->property)
                                ) {
                                    foreach ($action->property as $property) {
                                        $is_optional = false;
                                        if (substr($property, 0, 1) === '?') {
                                            $is_optional = true;
                                            $property = substr($property, 1);
                                        }
                                        $assertion = $property;
                                        $explode = explode(':', $property, 2);
                                        $compare = null;
                                        if (array_key_exists(1, $explode)) {
                                            $record_property = $node->get($explode[0]);
                                            $compare = $explode[1];
                                            $attribute = $explode[0];
                                            if ($compare) {
                                                $parse = new Parse($object, $object->data());
                                                $compare = $parse->compile($compare, $object->data());
                                                if ($record_property !== $compare) {
                                                    throw new Exception('Assertion failed: ' . $assertion . ' values [' . $record_property . ', ' . $compare . ']');
                                                }
                                            }
                                        }
                                        if (
                                            property_exists($action, 'object') &&
                                            property_exists($action->object, $property) &&
                                            is_array($action->object->$property)
                                        ) {
                                            d($property);
                                            ddd($action);
                                        }
                                        if (
                                            property_exists($action, 'object') &&
                                            property_exists($action->object, $property) &&
                                            !is_array($action->object->$property) &&
                                            property_exists($action->object->$property, 'output')
                                        ) {
                                            if (
                                                property_exists($action->object->$property, 'multiple') &&
                                                $action->object->$property->multiple === true &&
                                                $node->has($property)
                                            ) {
                                                $array = $node->get($property);
                                                if (is_array($array) || is_object($array)) {
                                                    $record[$property] = [];
                                                    foreach ($array as $child) {
                                                        if(Core::is_uuid($child)){
                                                            $record[$property][] = $child;
                                                            continue;
                                                        }
                                                        $child = new Storage($child);
                                                        $child_expose = [];
                                                        if (
                                                            property_exists($action->object->$property, 'object')
                                                        ) {
                                                            $child_expose[] = (object)[
                                                                'property' => $action->object->$property->output,
                                                                'object' => $action->object->$property->object,
                                                                'role' => $action->role,
                                                            ];
                                                        } else {
                                                            $child_expose[] = (object)[
                                                                'property' => $action->object->$property->output,
                                                                'role' => $action->role,
                                                            ];
                                                        }
                                                        $child_list = $this->expose_list(
                                                            [$child],
                                                            $child_expose,
                                                            $property,
                                                            'child',
                                                            $role,
                                                            $action->role
                                                        );
                                                        if (array_key_exists(0, $child_list)){
                                                            $record[$property][] = $child_list[0]->data();
                                                        }
                                                    }
                                                } else {
                                                    //leave intact for read without parse
                                                    $record[$property] = $array;
                                                }
                                            } elseif (
                                                $node->has($property)
                                            ) {
                                                $child = $node->get($property);
                                                if (!empty($child)) {
                                                    $record[$property] = null;
                                                    $child = new Storage($child);
                                                    $child_expose = [];
                                                    if (
                                                        property_exists($action->object->$property, 'object')
                                                    ) {
                                                        $child_expose[] = (object)[
                                                            'property' => $action->object->$property->output,
                                                            'object' => $action->object->$property->object,
                                                            'role' => $action->role,
                                                        ];
                                                    } else {
                                                        $child_expose[] = (object)[
                                                            'property' => $action->object->$property->output,
                                                            'role' => $action->role,
                                                        ];
                                                    }
                                                    $child_list = $this->expose_list(
                                                        [$child],
                                                        $child_expose,
                                                        $property,
                                                        'child',
                                                        $role,
                                                        $action->role
                                                    );
                                                    if(array_key_exists(0, $child_list)){
                                                        $record[$property] = $child_list[0]->data();
                                                    }
                                                }
                                                if (!array_key_exists($property, $record)) {
                                                    $record[$property] = null;
                                                }
                                            }
                                        } else {
                                            if ($node->has($property)) {
                                                $record[$property] = $node->get($property);
                                            }
                                        }
                                    }
                                }
                                $nodeList[$nr] = new Storage((object) $record);
                            }
                            break 3;
                        }
                    }
                }
            }
        }
        if($is_expose === false){
            $exception_role = [];
            foreach($roles as $role){
                $exception_role[] = $role->name;
            }
            throw new Exception(
                'No permission found for ' .
                str_replace('.', ':', Controller::name($class)) .
                ':' .
                str_replace('_', '.', $function) .
                ' for roles: ' . implode(', ', $exception_role)
            );
        }
        return $nodeList;
    }

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function expose_get(App $object, $name='', $attribute=''): mixed
    {
        $dir_expose = $object->config('project.dir.node') .
            'Expose' .
            $object->config('ds')
        ;
        $url = $dir_expose .
            $name .
            $object->config('extension.json')
        ;
        if(!File::exist($url)){
            throw new Exception('Expose: url (' . $url . ') not found for class: ' . $name);
        }
        $data = $object->data_read($url, sha1($url));
        $get = false;
        if($data){
            $get = $data->get($attribute);
        }
        if(empty($get)){
            throw new Exception('Expose: cannot find attribute (' . $attribute .') in class: ' . $name . ' url: ' . $url);
        }
        return $get;
    }

    /**
     * @throws ObjectException
     */
    private function expose_object_create_cli($depth=0): array
    {

        $result = [];
        $attribute = Cli::read('input', 'Object name (depth (' . $depth . ')): ');
        $attributes = [];
        while(!empty($attribute)){
            $multiple = Cli::read('input', 'Multiple (boolean): ');
            if(
                in_array(
                    $multiple,
                    [
                        'true',
                        1
                    ],
                    true
                )
            ) {
                $multiple = true;
            }
            if(
                in_array(
                    $multiple ,
                    [
                        'false',
                        0
                    ],
                    true
                 )
            ) {
                $multiple = false;
            }
            $expose = Cli::read('input', 'Expose (property): ');
            while(!empty($expose)){
                $attributes[] = $expose;
                $expose = Cli::read('input', 'Expose (property): ');
            }
            $object = [];
            $object['multiple'] = $multiple;
            $object['expose'] = $attributes;
            $object['object'] = $this->expose_object_create_cli(++$depth);
            if(empty($object['object'])){
                unset($object['object']);
            }
            $result[$attribute] = $object;
            $attribute = Cli::read('input', 'Object name (depth (' . --$depth . ')): ');
        }
        return $result;
    }

    /**
     * @throws ObjectException
     * @throws FileWriteException
     * @throws Exception
     */
    public function expose_create_cli(): void
    {
        $object = $this->object();
        if($object->config(Config::POSIX_ID) !== 0){
            return;
        }
        $class = Cli::read('input', 'Class: ');
        $url = $object->config('project.dir.data') .
            'Node' .
            $object->config('ds') .
            'Expose' .
            $object->config('ds') .
            $class .
            $object->config('extension.json')
        ;
        $expose = $object->data_read($url);
        $action = Cli::read('input', 'Action: ');
        $role = Cli::read('input', 'Role: ');
        $property = Cli::read('input', 'Property: ');
        $properties = [];
        while(!empty($property)){
            $properties[] = $property;
            $property = Cli::read('input', 'Property: ');
        }
        $objects = $this->expose_object_create_cli();
        if(!$expose){
            $expose = new Storage();
        }
        $list = (array) $expose->get($class . '.' . $action . '.expose');
        if(empty($list)){
            $list = [];
        } else {
            foreach ($list as $nr => $record){
                if(
                    is_array($record) &&
                    array_key_exists('role', $record)
                ){
                    if($record['role'] === $role){
                        unset($list[$nr]);
                    }
                }
                elseif(
                    is_object($record) &&
                    property_exists($record, 'role')
                ){
                    if($record->role === $role){
                        unset($list[$nr]);
                    }
                }
            }
        }
        $record = [];
        $record['role'] = $role;
        $record['property'] = $properties;
        $record['object'] = $objects;
        $list[] = $record;
        $result = [];
        foreach ($list as $record){
            $result[] = $record;
        }
        $expose->set($class . '.' . $action . '.expose', $result);
        $expose->write($url);
        $command = 'chown www-data:www-data ' . $url;
        exec($command);
        if($object->config('framework.environment') === Config::MODE_DEVELOPMENT){
            $command = 'chmod 666 ' . $url;
            exec($command);
        }
    }
}