<?php

namespace Raxon\Node\Trait;

use Raxon\App;
use Raxon\Config;

use Raxon\Module\Core;
use Raxon\Module\Data as Storage;
use Raxon\Module\Dir;
use Raxon\Module\File;

use Entity\Role as Entity;

use Exception;

use Raxon\Exception\DirectoryCreateException;
use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;

trait Role {

    /**
     * @throws ObjectException
     * @throws Exception
     */
    public function role_system(): false | object
    {
        $object = $this->object();
        if(
            in_array(
                $object->config(Config::POSIX_ID),
                [
                    0,
                    33,     //remove this, how to handle www-data events, middleware and filter
                ],
                true
            )
        ){
            $url = $object->config('project.dir.data') . 'Account' . $object->config('ds') . 'Role.System.json';
            $data = $object->data_read($url);
            if($data){
                $object->config('framework.role.system.uuid', $data->get('uuid'));
                return $data->data();
            }
        }
        return false;
    }

    /**
     * @throws Exception
     */
    public function role_has_permission($role, $permission=''): bool
    {
        if(
            property_exists($role, 'uuid') &&
            property_exists($role, 'permission') &&
            (
                is_array($role->permission) ||
                is_object($role->permission)
            )
        ){
            foreach ($role->permission as $record){
                if(
                    is_object($record) &&
                    property_exists($record, 'name') &&
                    $permission === $record->name
                ){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @throws ObjectException
     * @throws DirectoryCreateException
     * @throws FileWriteException
     * @throws Exception
     */
    public function role_system_create($package=''): void
    {
        $object = $this->object();
        if($object->config(Config::POSIX_ID) === 0){
            $url = $object->config('project.dir.data') . 'Account' . $object->config('ds') . 'Role.System.json';
            $url_package = $object->config('project.dir.vendor') . $package . '/Data/Role.System.json';
            if(File::exist($url_package)){
                if(File::exist($url)){
                    $data = $object->data_read($url);
                    $data_package = $object->data_read($url_package);
                    if($data && $data_package){
                        $name = $data_package->get('name');
                        if($data->get('name') === $name){
                            $permissions = $data->get('permission');
                            $list = [];
                            foreach($permissions as $nr => $permission){
                                if(
                                    is_object($permission) &&
                                    property_exists($permission, 'name')
                                ){
                                    $list[] = $permission->name;
                                }
                            }
                            $package_permissions = $data_package->get('permission');
                            if($package_permissions){
                                foreach($package_permissions as $permission){
                                    if(
                                        is_object($permission) &&
                                        property_exists($permission, 'name')
                                    ){
                                        if(!in_array($permission->name, $list, true)){
                                            $permissions[] = $permission;
                                        }
                                    }
                                }
                            }
                            $uuid = $data->get('uuid');
                            if(empty($uuid)){
                                $data->set('uuid', Core::uuid());
                            }
                            $data->set('permission', $permissions);
                            $data->write($url);
                            File::permission($object, [
                                'url' => $url
                            ]);
                        }
                    };
                } else {
                    $data = new Storage();
                    $data_package = $object->data_read($url_package);
                    if($data_package){
                        $data->data($data_package->data());
                        $dir = Dir::name($url);
                        Dir::create($dir, Dir::CHMOD);
                        $uuid = $data->get('uuid');
                        if(empty($uuid)){
                            $data->set('uuid', Core::uuid());
                        }
                        $data->write($url);
                        File::permission($object, [
                            'dir_data' => $object->config('project.dir.data'),
                            'dir' => $dir,
                            'url' => $url
                        ]);
                    }
                }
            }
        }
    }

    /**
     * @throws Exception
     */
    public function role(Entity $role, $options=[]){
        $object = $this->object();
        $result = $object->config('user.role');
        if($result === null){
            $permissions = $role->getPermissions();
            foreach($permissions as $nr => $permission){
                $permissions[$nr] = (object) [
                    'name' => $permission->getName()
                ];
            }
            $result = (object) [
                'name' => $role->getName(),
                'rank' => $role->getRank(),
                '#class' => 'Account.Role',
                'uuid' => Core::uuid(),
                'permission' => $permissions
            ];
            $object->config('user.role', $result);
        }
        return $result;
    }
}